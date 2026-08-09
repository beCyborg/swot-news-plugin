#!/usr/bin/env python3
"""Сбор данных Kagi News для плагина swot-news.

Режимы:
  --list-categories       живой каталог категорий батча, сгруппированный по
                          category_groups.json; JSON в stdout (+ --out файл)
  --build-clusters        детерминированная раскладка выбранных категорий
                          по 1–4 кластерам (LPT-балансировка по весам)
  --self-check            проверка площадки: конфиг, пути, tz, доступность API,
                          пропавшие из каталога категории
  --check-freshness       лёгкая проверка свежести батча (2 GET + чтение
                          frontmatter последнего выпуска), JSON в stdout
  (по умолчанию)          полный сбор: категории → stories с пагинацией →
                          кластер-файлы {out}/kagi_cluster_{key}.json (indent=1)
                          + {out}/kagi_meta.json; сводка JSON в stdout,
                          прогресс в stderr

Только stdlib, Python 3.9+. Данные Kagi News — CC BY-NC 4.0
(некоммерческое использование, атрибуция обязательна).
"""
import argparse
import concurrent.futures
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE = "https://news.kagi.com"
PAGE_LIMIT = 100
LICENSE = "Kagi News (news.kagi.com), CC BY-NC 4.0"
CONFIG_RELPATH = Path(".swot-news") / "config.json"
GROUPS_FILE = Path(__file__).resolve().parent / "category_groups.json"
DEFAULT_WEIGHT = 6  # вес неизвестной категории при балансировке кластеров
MAX_CLUSTERS = 4

for _stream in (sys.stdout, sys.stderr):  # Windows-консоль по умолчанию не UTF-8
    try:
        _stream.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001 — на старых Python метода нет, это не фатально
        pass


# ─────────────────────────────── HTTP ────────────────────────────────

def fetch_json(url, retries=2, timeout=30):
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 404:  # несуществующий batch/категория — ретрай бессмысленен
                break
            if attempt < retries:
                delay = 1 + attempt
                if e.code == 429:  # API просит подождать — уважаем Retry-After
                    try:
                        delay = min(float(e.headers.get("Retry-After", delay)), 30.0)
                    except (TypeError, ValueError):
                        pass
                time.sleep(delay)
        except Exception as e:  # noqa: BLE001 — retry на сетевой/JSON сбой
            last_err = e
            if attempt < retries:
                time.sleep(1 + attempt)
    raise RuntimeError(f"{url}: {last_err}")


# ────────────────────────── конфиг и площадка ────────────────────────

def find_config(explicit=None):
    """--config → env SWOT_NEWS_CONFIG → ./.swot-news/config.json → вверх по дереву."""
    tried = []
    if explicit:
        p = Path(explicit).expanduser()
        tried.append(str(p))
        return (p if p.is_file() else None), tried
    env = os.environ.get("SWOT_NEWS_CONFIG")
    if env:
        p = Path(env).expanduser()
        tried.append(str(p))
        if p.is_file():
            return p, tried
    here = Path.cwd().resolve()
    for d in [here, *here.parents][:8]:
        p = d / CONFIG_RELPATH
        tried.append(str(p))
        if p.is_file():
            return p, tried
    for p in sorted(here.glob(f"*/{CONFIG_RELPATH.as_posix()}")):
        tried.append(str(p))
        if p.is_file():
            return p, tried
    return None, tried


def load_config(explicit=None, required=True):
    path, tried = find_config(explicit)
    if path is None:
        if not required:
            return None, None
        json.dump({
            "error": "config_not_found",
            "hint": "Конфиг swot-news не найден. Запусти /swot-news:setup в рабочей папке "
                    "или укажи путь: --config <путь>/.swot-news/config.json "
                    "(либо переменную окружения SWOT_NEWS_CONFIG).",
            "searched": tried,
        }, sys.stdout, ensure_ascii=False, indent=1)
        print()
        sys.exit(2)
    try:
        cfg = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        json.dump({"error": "config_unreadable", "path": str(path), "detail": str(e)},
                  sys.stdout, ensure_ascii=False, indent=1)
        print()
        sys.exit(2)
    cfg.setdefault("base_dir", str(path.parent.parent))
    cfg.setdefault("issues_dir", "Выпуски")
    cfg.setdefault("note_prefix", "SWOT_")
    cfg.setdefault("lang", "en")
    cfg.setdefault("batch_hour_utc", 12)
    cfg.setdefault("timezone", None)
    cfg.setdefault("clusters", {})
    cfg["_path"] = str(path)
    return cfg, path


def get_tz(name):
    """ZoneInfo(name) или None (= системное локальное время).

    На Windows без пакета tzdata база таймзон отсутствует — молча падаем
    на локальное время, это корректнее, чем падать целиком.
    """
    if not name:
        return None
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception:  # noqa: BLE001 — нет zoneinfo/tzdata или кривое имя
        return None


def local_today(tz_name):
    tz = get_tz(tz_name)
    return datetime.now(tz).date() if tz else datetime.now().date()


def notes_dir_of(cfg):
    return Path(cfg["base_dir"]).expanduser() / cfg["issues_dir"]


def out_dir_of(args):
    d = Path(args.out_dir).expanduser() if args.out_dir \
        else Path(tempfile.gettempdir()) / "swot-news"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ────────────────────────── каталог категорий ────────────────────────

def load_groups():
    data = json.loads(GROUPS_FILE.read_text(encoding="utf-8"))
    groups = data["groups"]
    order = data.get("_order", list(groups))
    index = {}
    for gid, g in groups.items():
        for cat in g.get("categories", []):
            index[cat] = gid
    return groups, order, index


def guess_group(cat_id, cat_name):
    """Категория, которой нет в статической карте: гео-лента или «прочее»."""
    return "local" if "|" in cat_name or "_|_" in cat_id else "other"


def live_catalog(lang):
    batch = fetch_json(f"{BASE}/api/batches/latest?lang={lang}")
    cats_meta = fetch_json(f"{BASE}/api/batches/{batch['id']}/categories?lang={lang}")
    return batch, cats_meta.get("categories", [])


def list_categories(args):
    batch, cats = live_catalog(args.lang)
    groups, order, index = load_groups()
    buckets = {gid: [] for gid in order}
    unknown = 0
    for c in cats:
        cid, cname = c["categoryId"], c.get("categoryName", c["categoryId"])
        gid = index.get(cid)
        if gid is None:
            gid = guess_group(cid, cname)
            unknown += 1
        buckets.setdefault(gid, []).append({
            "id": cid, "name": cname,
            "w": c.get("clusterCount") or DEFAULT_WEIGHT,
        })
    out_groups = []
    for gid in order:
        items = sorted(buckets.get(gid, []), key=lambda x: x["name"])
        if not items:
            continue
        meta = groups.get(gid, {})
        out_groups.append({
            "id": gid,
            "label": meta.get("label", gid),
            "hint": meta.get("hint", ""),
            "count": len(items),
            "categories": items,
        })
    result = {
        "batch_id": batch["id"], "dateSlug": batch.get("dateSlug"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "lang": args.lang,
        "total_categories": len(cats),
        "unknown_to_groups": unknown,
        "groups": out_groups,
        "license": LICENSE,
    }
    if args.out:
        p = Path(args.out).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"Каталог записан: {p}", file=sys.stderr)
        result = {**{k: v for k, v in result.items() if k != "groups"},
                  "out": str(p),
                  "groups_summary": [{"id": g["id"], "label": g["label"], "count": g["count"]}
                                     for g in out_groups]}
    return result


# ─────────────────────── балансировка кластеров ──────────────────────

def lpt_assign(items, keys, by_group):
    """LPT-раскладка: тяжёлые куски первыми, каждый — в наименее загруженный кластер.

    by_group=True склеивает категории одного тематического блока в один кусок —
    аналитику проще дедуплицировать связанные сюжеты. Блок тяжелее 1.3 средней
    нагрузки рассыпается на категории, иначе он один перекосил бы кластер.
    Тай-брейки по id и индексу кластера делают раскладку воспроизводимой.
    """
    total = sum(i["w"] for i in items)
    target = total / len(keys) if keys else total
    if by_group:
        by_gid = {}
        for it in items:
            by_gid.setdefault(it["group"], []).append(it)
        chunks = []
        for gid, members in sorted(by_gid.items()):
            gw = sum(m["w"] for m in members)
            if gw > target * 1.3 and len(members) > 1:
                chunks.extend([[m] for m in members])
            else:
                chunks.append(members)
    else:
        chunks = [[it] for it in items]

    loads = {k: 0 for k in keys}
    assign = {k: [] for k in keys}
    for chunk in sorted(chunks, key=lambda c: (-sum(m["w"] for m in c), c[0]["id"])):
        k = min(keys, key=lambda key: (loads[key], keys.index(key)))
        assign[k].extend(chunk)
        loads[k] += sum(m["w"] for m in chunk)
    nonzero = [v for v in loads.values() if v > 0]
    ratio = (max(nonzero) / min(nonzero)) if nonzero else 0.0
    return assign, loads, ratio


def build_clusters(args):
    wanted = [c.strip().lower() for c in (args.categories or "").split(",") if c.strip()]
    if not wanted:
        raise RuntimeError("--build-clusters требует --categories cat1,cat2,...")
    seen, ordered = set(), []
    for c in wanted:  # дедуп с сохранением порядка ввода
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    n = max(1, min(MAX_CLUSTERS, args.n))

    batch, cats = live_catalog(args.lang)
    live = {c["categoryId"].lower(): c for c in cats}
    groups, order, index = load_groups()

    missing = [c for c in ordered if c not in live]
    present = [c for c in ordered if c in live]
    items = [{
        "id": c,
        "name": live[c].get("categoryName", c),
        "w": live[c].get("clusterCount") or DEFAULT_WEIGHT,
        "group": index.get(c) or guess_group(c, live[c].get("categoryName", c)),
    } for c in present]

    keys = [chr(ord("a") + i) for i in range(n)]
    assign, loads, ratio = lpt_assign(items, keys, by_group=True)
    if ratio > 1.5:  # тематическая склейка перекосила — рассыпаем по категориям
        assign, loads, ratio = lpt_assign(items, keys, by_group=False)

    clusters = {}
    for k in keys:
        members = sorted(assign[k], key=lambda x: x["id"])
        counts = {}
        for m in members:
            counts[m["group"]] = counts.get(m["group"], 0) + m["w"]
        top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:2]
        label = ", ".join(groups.get(g, {}).get("label", g) for g, _ in top) or "Смешанный"
        clusters[k] = {
            "label": label,
            "categories": [m["id"] for m in members],
            "weight": loads[k],
            "names": [m["name"] for m in members],
        }
    return {
        "n": n,
        "requested": len(ordered),
        "assigned": len(items),
        "categories_missing": missing,
        "balance_ratio": round(ratio, 2),
        "balanced": ratio <= 1.5,
        "clusters": clusters,
        "batch_id": batch["id"],
    }


# ──────────────────────────── свежесть ───────────────────────────────

def read_last_note(notes_dir, prefix):
    """Последний выпуск {prefix}*.md и его batch_id/batch_created_at из frontmatter."""
    d = Path(notes_dir)
    if not d.is_dir():
        return None
    notes = sorted(d.glob(f"{prefix}*.md"))
    if not notes:
        return None
    result = {"note": notes[-1].name, "batch_id": None, "batch_created_at": None}
    for note in reversed(notes):
        head = note.read_text(encoding="utf-8", errors="replace")[:4096]
        if not head.startswith("---"):
            continue
        fm = head.split("---", 2)[1] if head.count("---") >= 2 else ""
        bid = re.search(r'^batch_id:\s*["\']?([\w.-]+)', fm, re.M)
        bca = re.search(r'^batch_created_at:\s*["\']?([\w:.+-]+)', fm, re.M)
        if bid or bca:
            result.update({
                "note_with_batch": note.name,
                "batch_id": bid.group(1) if bid else None,
                "batch_created_at": bca.group(1) if bca else None,
            })
            break
    return result


def fresh_by_time(created_at_iso, tz_name, hour_utc):
    """Вторичный предикат: батч создан не раньше {hour_utc}:00 UTC сегодняшнего дня."""
    created = datetime.fromisoformat(created_at_iso.replace("Z", "+00:00"))
    today = local_today(tz_name)
    cutoff = datetime(today.year, today.month, today.day, hour_utc, 0, tzinfo=timezone.utc)
    return created >= cutoff


def wanted_categories(cfg):
    out = []
    for cl in cfg.get("clusters", {}).values():
        out.extend(c.lower() for c in cl.get("categories", []))
    return out


def check_freshness(cfg, lang):
    batch, cats = live_catalog(lang)
    live_ids = {c["categoryId"].lower() for c in cats}
    wanted = wanted_categories(cfg)
    missing = sorted(set(wanted) - live_ids)
    last = read_last_note(notes_dir_of(cfg), cfg["note_prefix"])
    by_time = fresh_by_time(batch["createdAt"], cfg["timezone"], cfg["batch_hour_utc"])
    if last and last.get("batch_id"):
        fresh = batch["id"] != last["batch_id"]
        reason = ("batch_id отличается от последнего выпуска" if fresh
                  else f"batch_id совпадает с {last.get('note_with_batch')}")
    elif last and last.get("batch_created_at"):
        fresh = batch["createdAt"] != last["batch_created_at"]
        reason = "сравнение по batch_created_at (batch_id в выпуске нет)"
    else:
        fresh = by_time
        reason = (f"нет выпусков с batch-метаданными; временной предикат "
                  f"(createdAt >= сегодня {cfg['batch_hour_utc']}:00 UTC)")
    return {
        "fresh": fresh,
        "reason": reason,
        "batch_id": batch["id"],
        "dateSlug": batch.get("dateSlug"),
        "createdAt": batch["createdAt"],
        "fresh_by_time": by_time,
        "today": local_today(cfg["timezone"]).isoformat(),
        "last_note": last["note"] if last else None,
        "last_note_batch_id": last.get("batch_id") if last else None,
        "categories_wanted": len(wanted),
        "categories_missing": missing,
        "categories_missing_share": round(len(missing) / len(wanted), 3) if wanted else 0.0,
    }


# ───────────────────────────── self-check ────────────────────────────

def self_check(cfg, args):
    lang = args.lang
    problems, notes = [], []
    base = Path(cfg["base_dir"]).expanduser()
    if not base.is_dir():
        problems.append(f"base_dir не существует: {base}")
    nd = notes_dir_of(cfg)
    if not nd.is_dir():
        notes.append(f"папка выпусков будет создана при первом прогоне: {nd}")
    clusters = cfg.get("clusters", {})
    if not 1 <= len(clusters) <= MAX_CLUSTERS:
        problems.append(f"clusters: должно быть 1–{MAX_CLUSTERS}, сейчас {len(clusters)}")
    wanted = wanted_categories(cfg)
    if not wanted:
        problems.append("в кластерах нет ни одной категории")
    dups = sorted({c for c in wanted if wanted.count(c) > 1})
    if dups:
        problems.append(f"категории продублированы между кластерами: {dups}")

    tz_name = cfg.get("timezone")
    tz_ok = bool(get_tz(tz_name)) if tz_name else None
    if tz_name and not tz_ok:
        notes.append(f"таймзона '{tz_name}' не разрешилась — используется системное "
                     f"локальное время (на Windows поможет: pip install tzdata)")

    api_ok, missing, batch_id = False, [], None
    try:
        batch, cats = live_catalog(lang)
        api_ok, batch_id = True, batch["id"]
        live_ids = {c["categoryId"].lower() for c in cats}
        missing = sorted(set(wanted) - live_ids)
    except Exception as e:  # noqa: BLE001
        problems.append(f"Kagi API недоступен: {e}")

    out = out_dir_of(args)
    try:
        probe = out / ".write-probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        out_writable = True
    except Exception as e:  # noqa: BLE001
        out_writable = False
        problems.append(f"временная папка недоступна для записи ({out}): {e}")

    share = round(len(missing) / len(wanted), 3) if wanted else 0.0
    if share > 0.2:
        notes.append(f"из каталога пропало {len(missing)} из {len(wanted)} категорий "
                     f"({int(share * 100)}%) — стоит перепройти выбор: /swot-news:setup --recheck")
    return {
        "ok": not problems,
        "config": cfg["_path"],
        "python": sys.version.split()[0],
        "base_dir": str(base),
        "issues_dir": str(nd),
        "out_dir": str(out),
        "out_writable": out_writable,
        "timezone": tz_name,
        "timezone_resolved": tz_ok,
        "today": local_today(tz_name).isoformat(),
        "clusters": {k: len(v.get("categories", [])) for k, v in clusters.items()},
        "categories_wanted": len(wanted),
        "categories_missing": missing,
        "categories_missing_share": share,
        "enricher": cfg.get("enricher", {}).get("tool"),
        "obsidian": cfg.get("obsidian", {}).get("enabled"),
        "api_ok": api_ok,
        "batch_id": batch_id,
        "problems": problems,
        "notes": notes,
    }


# ─────────────────────────── полный сбор ─────────────────────────────

def get_category_stories(batch_id, cat, lang):
    """Все stories категории с пагинацией (дефолт API: 50 у batchId-варианта,
    12 у /latest-варианта; ставим явно 100 — документированный максимум)."""
    stories, offset, reported_total, data = [], 0, None, {}
    while True:
        data = fetch_json(
            f"{BASE}/api/batches/{batch_id}/categories/{cat['id']}/stories"
            f"?lang={lang}&limit={PAGE_LIMIT}&offset={offset}")
        page = data.get("stories", [])
        reported_total = data.get("totalStories", reported_total)
        stories.extend(page)
        if not page or len(page) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
    return {
        "name": data.get("categoryName", cat["categoryId"]),
        "story_count": len(stories),
        "total_stories_reported": reported_total,
        "stories": stories,
    }


def full_fetch(cfg, args):
    lang = args.lang
    batch = fetch_json(f"{BASE}/api/batches/latest?lang={lang}")
    batch_id, created_at, date_slug = batch["id"], batch["createdAt"], batch.get("dateSlug")
    # Всё дальше — строго по пиненному batch_id: /latest между запросами может смениться
    cats_meta = fetch_json(f"{BASE}/api/batches/{batch_id}/categories?lang={lang}")
    all_cats = cats_meta.get("categories", [])
    print(f"Батч {date_slug} ({batch_id}), категорий в батче: {len(all_cats)}", file=sys.stderr)

    clusters = cfg["clusters"]
    if args.all:
        wanted = None
    elif args.categories:
        wanted = {c.strip().lower() for c in args.categories.split(",") if c.strip()}
    else:
        wanted = set(wanted_categories(cfg))
    selected = [c for c in all_cats if wanted is None or c["categoryId"].lower() in wanted]
    missing = sorted(wanted - {c["categoryId"].lower() for c in selected}) if wanted else []
    if missing:
        print(f"Нет в батче (пропущены): {missing}", file=sys.stderr)
    print(f"К сбору: {len(selected)} категорий, потоков: {args.threads}", file=sys.stderr)

    try:
        chaos = fetch_json(f"{BASE}/api/batches/{batch_id}/chaos?lang={lang}")
    except Exception as e:  # noqa: BLE001
        print(f"chaos недоступен: {e}", file=sys.stderr)
        chaos = {"chaosIndex": None, "chaosDescription": "unavailable"}
    onthisday = None
    if cats_meta.get("hasOnThisDay"):
        try:
            onthisday = fetch_json(f"{BASE}/api/batches/{batch_id}/onthisday?lang={lang}")
        except Exception as e:  # noqa: BLE001
            print(f"onthisday недоступен: {e}", file=sys.stderr)

    results, truncated = {}, []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as ex:
        futures = {ex.submit(get_category_stories, batch_id, c, lang): c for c in selected}
        for fut in concurrent.futures.as_completed(futures):
            cat = futures[fut]
            cid = cat["categoryId"].lower()
            try:
                res = fut.result()
            except Exception as e:  # noqa: BLE001
                print(f"  ОШИБКА {cid}: {e}", file=sys.stderr)
                res = {"name": cid, "story_count": 0, "total_stories_reported": None,
                       "stories": [], "error": str(e)}
            rep = res["total_stories_reported"]
            if rep is not None and res["story_count"] < rep:
                truncated.append({"category": cid, "got": res["story_count"], "reported": rep})
            results[cid] = res
            print(f"  {cid}: {res['story_count']} stories", file=sys.stderr)

    cluster_index = {c.lower(): key for key, cl in clusters.items()
                     for c in cl.get("categories", [])}
    out_dir = out_dir_of(args)
    keys = list(clusters)
    last_key = keys[-1] if keys else "a"
    unmatched = sorted(cid for cid in results if cid not in cluster_index)
    files, cluster_summary = [], {}
    for key in keys:
        cl = clusters[key]
        cats = {cid: results[cid] for cid in (c.lower() for c in cl.get("categories", []))
                if cid in results}
        if key == last_key and unmatched:  # категории вне кластеров (--all/--categories)
            cats.update({cid: results[cid] for cid in unmatched})
        path = out_dir / f"kagi_cluster_{key}.json"
        payload = {
            "cluster": key, "label": cl.get("label", key),
            "batch_id": batch_id, "batch_created_at": created_at, "dateSlug": date_slug,
            "categories": cats,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        files.append(str(path))
        cluster_summary[key] = {
            "label": cl.get("label", key),
            "file": str(path),
            "categories": sorted(cats),
            "stories": sum(v["story_count"] for v in cats.values()),
        }

    total_stories = sum(v["story_count"] for v in results.values())
    total_articles = sum(len(s.get("articles", [])) for v in results.values()
                         for s in v["stories"])
    meta = {
        "batch_id": batch_id, "batch_created_at": created_at, "dateSlug": date_slug,
        "lang": lang, "fetched_at": datetime.now(timezone.utc).isoformat(),
        "today": local_today(cfg["timezone"]).isoformat(),
        "chaos_index": chaos, "onthisday": onthisday,
        "total_stories": total_stories, "total_articles": total_articles,
        "categories_fetched": len(results), "clusters": cluster_summary,
        "categories_missing": missing,
        "truncated_categories": truncated, "license": LICENSE,
    }
    meta_path = out_dir / "kagi_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    files.append(str(meta_path))
    print(f"Итого: {total_stories} stories, {total_articles} articles", file=sys.stderr)
    if truncated:
        print(f"ВНИМАНИЕ, обрезанные категории: {truncated}", file=sys.stderr)

    wanted_n = len(wanted) if wanted else len(selected)
    return {
        "batch_id": batch_id, "dateSlug": date_slug, "batch_created_at": created_at,
        "today": local_today(cfg["timezone"]).isoformat(),
        "chaos_index": chaos.get("chaosIndex"),
        "total_stories": total_stories, "total_articles": total_articles,
        "categories_fetched": len(results),
        "categories_missing": missing,
        "categories_missing_share": round(len(missing) / wanted_n, 3) if wanted_n else 0.0,
        "clusters": {k: {"label": v["label"], "file": v["file"],
                         "categories": v["categories"], "stories": v["stories"]}
                     for k, v in cluster_summary.items()},
        "meta_file": str(meta_path),
        "truncated_categories": truncated, "files": files,
    }


# ──────────────────────────────── CLI ────────────────────────────────

def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", help="путь к .swot-news/config.json")
    p.add_argument("--lang", help="язык батча (дефолт из конфига, иначе en)")
    p.add_argument("--all", action="store_true", help="все категории батча")
    p.add_argument("--categories", help="свой список: world,ai,poland")
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--out-dir", help="куда класть кластер-файлы (дефолт: temp/swot-news)")
    p.add_argument("--out", help="файл для --list-categories")
    p.add_argument("-n", type=int, default=4, help="число кластеров для --build-clusters (1–4)")
    p.add_argument("--check-freshness", action="store_true")
    p.add_argument("--list-categories", action="store_true")
    p.add_argument("--build-clusters", action="store_true")
    p.add_argument("--self-check", action="store_true")
    args = p.parse_args()

    needs_config = not (args.list_categories or args.build_clusters)
    cfg = None
    if needs_config:
        cfg, _ = load_config(args.config, required=True)
        if not args.lang:
            args.lang = cfg["lang"]
    elif not args.lang:
        cfg_opt, _ = load_config(args.config, required=False)
        args.lang = (cfg_opt or {}).get("lang", "en")

    try:
        if args.list_categories:
            result = list_categories(args)
        elif args.build_clusters:
            result = build_clusters(args)
        elif args.self_check:
            result = self_check(cfg, args)
        elif args.check_freshness:
            result = check_freshness(cfg, args.lang)
        else:
            result = full_fetch(cfg, args)
    except Exception as e:  # noqa: BLE001
        json.dump({"error": str(e)}, sys.stdout, ensure_ascii=False)
        print()
        sys.exit(1)
    json.dump(result, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()

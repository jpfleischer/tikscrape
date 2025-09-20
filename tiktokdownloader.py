#!/usr/bin/env python3
import os, json, time, re, sys, pathlib, itertools
import urllib.parse

from typing import Any, Dict, List, Optional
import requests
from dateutil import tz
from datetime import datetime
from typing import Optional

import tikhub_sdk_v2
from tikhub_sdk_v2.rest import ApiException
from dotenv import load_dotenv

load_dotenv()  # reads .env from current working directory

API_HOST = "https://api.tikhub.io"


UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")

# ---------- utilities ----------
def _json(obj) -> Dict[str, Any]:
    if hasattr(obj, "json"):
        return obj.json()
    if isinstance(obj, (bytes, bytearray)):
        try:
            return json.loads(obj.decode("utf-8"))
        except Exception:
            return {}
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except Exception:
            return {}
    return obj or {}

def _first(iterable, default=None):
    return next((x for x in iterable if x is not None), default)

def _ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _expand_share_url(url: str, max_hops: int = 5) -> str:
    """
    Follow redirects for TikTok short links (/t/, vm., vt.) and return the final URL.
    Uses a real GET + desktop UA because HEAD often won’t expand them.
    """
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    cur = url
    for _ in range(max_hops):
        try:
            r = s.get(cur, allow_redirects=True, timeout=15)
            r.raise_for_status()
            nxt = r.url or cur
            if nxt == cur and "text/html" in r.headers.get("Content-Type", "") and r.text:
                # handle simple meta-refresh cases
                m = re.search(r'http-equiv=["\']refresh["\'][^>]*url=([^"\'> ]+)', r.text, re.I)
                if m:
                    nxt = urllib.parse.unquote(m.group(1))
            if nxt == cur:
                return cur
            cur = nxt
            time.sleep(0.1)
        except Exception:
            return cur
    return cur


def _safe_filename(s: str, max_len=120) -> str:
    s = re.sub(r"[^\w\-. ]+", "_", s).strip()
    return (s[:max_len]).rstrip("._-") or "tiktok"

def _utc_to_local(ts: int) -> str:
    # TikTok timestamps are usually seconds since epoch (UTC)
    dt = datetime.utcfromtimestamp(int(ts)).replace(tzinfo=tz.UTC).astimezone(tz.tzlocal())
    return dt.isoformat()


def _collect_caption_urls_from_detail(detail: dict):
    d = (detail.get("data") if isinstance(detail, dict) else None) or detail or {}
    urls = []

    # existing places
    urls += [x.get("url") for x in (((d.get("aweme_detail") or {}).get("video") or {}).get("subtitleInfos") or []) if isinstance(x, dict)]
    urls += [x.get("url") for x in ((d.get("video") or {}).get("subtitles") or []) if isinstance(x, dict)]
    urls += [x.get("url") for x in (d.get("subtitles") or []) if isinstance(x, dict)]
    if isinstance(d.get("subtitle_url"), str):
        urls.append(d["subtitle_url"])
    urls += [x.get("url") for x in (d.get("captions") or []) if isinstance(x, dict)]
    cap = d.get("caption") or {}
    if isinstance(cap, dict) and isinstance(cap.get("url"), str):
        urls.append(cap["url"])

    # NEW: TikTok App V3 → video.cla_info.caption_infos[*]
    aweme = (d.get("aweme_detail") or {})
    v = (aweme.get("video") or {})
    cla = (v.get("cla_info") or {})
    infos = cla.get("caption_infos") or []
    for ci in infos:
        if not isinstance(ci, dict):
            continue
        # prefer SmartPlayerSubtitleRedirect if present
        lst = ci.get("url_list") or []
        for u in lst:
            if isinstance(u, str) and "SmartPlayerSubtitleRedirect" in u:
                urls.append(u)
        # then fall back to the single url field
        u = ci.get("url")
        if isinstance(u, str):
            urls.append(u)

    # de-dup + sanity
    seen, out = set(), []
    for u in urls:
        if isinstance(u, str) and u.startswith("http") and u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _preferred_caption_urls(detail: dict):
    # pull rich objects for sorting
    d = (detail.get("data") if isinstance(detail, dict) else None) or detail or {}
    out = []
    v = (d.get("aweme_detail") or {}).get("video") or {}
    infos = ((v.get("cla_info") or {}).get("caption_infos") or [])
    for ci in infos:
        if not isinstance(ci, dict):
            continue
        lang = (ci.get("language_code") or ci.get("lang") or "").lower()
        fmt  = (ci.get("caption_format") or "").lower()
        urls = (ci.get("url_list") or []) + ([ci.get("url")] if ci.get("url") else [])
        for u in urls:
            if isinstance(u, str) and u.startswith("http"):
                out.append((lang, fmt, u))

    # score: english first, webvtt first, SmartPlayerRedirect first
    def score(t):
        lang, fmt, u = t
        is_en = int(lang in ("en", "eng-us", "en-us"))
        is_vtt = int(fmt == "webvtt")
        is_redirect = int("SmartPlayerSubtitleRedirect" in u)
        return (is_en, is_vtt, is_redirect)

    out.sort(key=score, reverse=True)
    return [u for _, _, u in out]

# ---------- SDK init ----------
def make_client():
    api_key = os.getenv("TIKHUB_API_KEY")
    if not api_key:
        print("ERROR: Please export TIKHUB_API_KEY", file=sys.stderr)
        sys.exit(1)

    cfg = tikhub_sdk_v2.Configuration(host=API_HOST)
    # README shows this exact pattern (prefix with 'Bearer ') 
    cfg.access_token = f"Bearer {api_key}"
    return tikhub_sdk_v2.ApiClient(cfg)

# ---------- dynamic method finder ----------
def find_method(api_obj, *must_contain):
    """Finds the first callable attribute whose name contains all substrings."""
    name = next(
        (n for n in dir(api_obj)
         if callable(getattr(api_obj, n))
         and all(sub in n for sub in must_contain)),
        None
    )
    return getattr(api_obj, name) if name else None

def resolve_aweme_id_via_get_endpoint(api_client, share_url: str) -> Optional[str]:
    """
    Use TikHub 'get_aweme_id' endpoints ONLY (SDK if present, else raw REST).
    No local redirect expansion.
    """
    # --- SDK paths (TikTok-Web and Douyin-Web) ---
    sdk_classes = []
    for name in ("TikTokWebAPIApi", "TikTokWebApi", "DouyinWebAPIApi", "DouyinWebApi"):
        cls = getattr(tikhub_sdk_v2, name, None)
        if cls:
            try:
                sdk_classes.append(cls(api_client))
            except Exception:
                pass

    for api in sdk_classes:
        m = find_method(api, "get", "aweme", "id") or find_method(api, "aweme", "id")
        if not m:
            continue
        # support different arg names across builds
        for param in ("url", "share_url", "link"):
            try:
                resp = m(**{param: share_url, "_preload_content": False})
                data = _json(resp)
                aweme_id = _first([data.get("aweme_id"), (data.get("data") or {}).get("aweme_id")])
                if aweme_id:
                    return str(aweme_id)
            except TypeError:
                continue
            except Exception:
                continue

    # --- Raw REST fallbacks (mirrors) ---
    api_key = os.getenv("TIKHUB_API_KEY")
    if not api_key:
        return None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        # UA is fine to keep; the API handles short links serverside
        "User-Agent": UA,
    }
    endpoints = [
        f"{API_HOST}/api/v1/tiktok/web/get_aweme_id",
        f"{API_HOST}/tiktok/web/get_aweme_id",
        f"{API_HOST}/api/v1/douyin/web/get_aweme_id",
        f"{API_HOST}/douyin/web/get_aweme_id",
    ]
    for ep in endpoints:
        try:
            r = requests.get(ep, params={"url": share_url}, headers=headers, timeout=20)
            if r.status_code == 200:
                data = _json(r.content)
                aweme_id = _first([data.get("aweme_id"), (data.get("data") or {}).get("aweme_id")])
                if aweme_id:
                    return str(aweme_id)
        except Exception:
            continue

    return None



def _clean_url(u: str) -> str:
    return (u or "").strip().strip("'").strip('"')


def resolve_aweme_id(api_client, share_url: str) -> str:
    """
    Only use TikHub endpoints. Do NOT expand URLs locally.
    1) TikTok-Web:  GET /api/v1/tiktok/web/get_aweme_id?url=...
    2) TikTok-App-V3 (fallback): GET /api/v1/tiktok/app/v3/fetch_one_video_by_share_url?share_url=...
    """
    api_key = os.getenv("TIKHUB_API_KEY")
    if not api_key:
        raise RuntimeError("TIKHUB_API_KEY is not set")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": UA,
    }
    url = (share_url or "").strip().strip("'").strip('"')

    # --- 1) TikTok-Web get_aweme_id
    web_ep = f"{API_HOST}/api/v1/tiktok/web/get_aweme_id"
    try:
        r = requests.get(web_ep, params={"url": url}, headers=headers, timeout=20)
        if r.status_code == 200:
            data = _json(r.content)
            # NEW: handle a bare string response
            if isinstance(data, str):
                s = data.strip().strip('"').strip()
                if s.isdigit():
                    return s
            # handle dict shapes
            aweme_id = _first([
                (data.get("aweme_id") if isinstance(data, dict) else None),
                ((data.get("data") if isinstance(data, dict) else None) if isinstance(data.get("data"), str) and data.get("data").isdigit() else None),
                (((data.get("data") or {}).get("aweme_id")) if isinstance(data, dict) and isinstance(data.get("data"), dict) else None),
            ])
            if aweme_id:
                return str(aweme_id)
            else:
                print(f"[debug] web/get_aweme_id 200 but no aweme_id. body={str(data)[:300]}")

        else:
            print(f"[debug] web/get_aweme_id HTTP {r.status_code}: {r.text[:300]}")
    except Exception as e:
        print(f"[debug] web/get_aweme_id exception: {e}")

    # --- 2) App-V3 by-share-url fallback → extract aweme_id from video data
    app_ep = f"{API_HOST}/api/v1/tiktok/app/v3/fetch_one_video_by_share_url"
    try:
        r2 = requests.get(app_ep, params={"share_url": url}, headers=headers, timeout=30)
        if r2.status_code == 200:
            data2 = _json(r2.content)
            # NEW: if the whole payload is just the id string
            if isinstance(data2, str):
                s = data2.strip().strip('"').strip()
                if s.isdigit():
                    return s
            d = data2.get("data") if isinstance(data2, dict) else None
            if isinstance(d, str) and d.isdigit():
                return d
            if not isinstance(d, dict):
                d = data2 if isinstance(data2, dict) else {}
            aweme_id = _first([
                ((d.get("aweme_detail") or {}).get("aweme_id")),
                ((d.get("aweme_detail") or {}).get("aweme_id_str")),
                ((d.get("aweme") or {}).get("aweme_id")),
                d.get("aweme_id"),
            ])
            if aweme_id:
                return str(aweme_id)
            else:
                print(f"[debug] app/v3/fetch_one_video_by_share_url 200 but no aweme_id. body sample={str(data2)[:500]}")

        else:
            print(f"[debug] app/v3/fetch_one_video_by_share_url HTTP {r2.status_code}: {r2.text[:300]}")
    except Exception as e:
        print(f"[debug] app/v3/fetch_one_video_by_share_url exception: {e}")

    raise RuntimeError("Could not resolve aweme_id from the given URL via API.")


# ---------- fetch video details ----------
def fetch_video_detail(api_client, aweme_id: str) -> Dict[str, Any]:
    """
    Try Web ‘Get single video data’, then AppV3 ‘Get single video data’.
    """
    # Web ‘Get single video data’ (docs list it explicitly) 
    web = tikhub_sdk_v2.TikTokWebAPIApi(api_client)
    m = find_method(web, "tiktok_web", "fetch", "one", "video", "get")
    if not m:
        m = find_method(web, "tiktok_web", "fetch_single", "video")
    if m:
        try:
            resp = m(aweme_id=aweme_id, _preload_content=False)
            data = _json(resp)
            if data:
                return data
        except ApiException:
            pass

    # AppV3 fallback (docs: TikTok-App-V3-API ‘Get single video data’) 
    app = tikhub_sdk_v2.TikTokAppV3APIApi(api_client)
    m2 = find_method(app, "tiktok_app_v3", "fetch", "one", "video", "get")
    if m2:
        resp = m2(aweme_id=aweme_id, _preload_content=False)
        return _json(resp)

    raise RuntimeError("Could not fetch video detail.")

# ---------- highest quality play URL (optional helper) ----------
def fetch_highest_quality_url(api_client, aweme_id: str) -> Optional[str]:
    # Docs list a “Get the highest quality play URL of the video” under App V3 
    app = tikhub_sdk_v2.TikTokAppV3APIApi(api_client)
    m = find_method(app, "tiktok_app_v3", "highest", "quality", "play", "url")
    if m:
        try:
            resp = m(aweme_id=aweme_id, _preload_content=False)
            data = _json(resp)
            return _first([
                data.get("play_url"),
                data.get("data", {}).get("play_url")
            ])
        except ApiException:
            return None
    return None

# ---------- pick a playable video URL from detail ----------
def extract_play_url(detail: Dict[str, Any]) -> Optional[str]:
    candidates = []
    # Common TikTok shapes
    d = detail
    # App style
    candidates += [
        _first([
            _first(d.get("data", {}).get("aweme_detail", {}).get("video", {}).get("play_addr", {}).get("url_list", [])),
            _first(d.get("data", {}).get("video", {}).get("play_addr", {}).get("url_list", [])),
        ])
    ]
    # Web style
    candidates += [
        _first(d.get("data", {}).get("video", {}).get("play_url_list", [])),
        d.get("play_url"),
    ]
    return _first(candidates)

# ---------- captions/subtitles from detail ----------
def extract_caption_urls(detail: Dict[str, Any]) -> List[str]:
    """
    TikTok sometimes exposes subtitles as:
      - data.aweme_detail.video.subtitleInfos[*].url
      - data.video.subtitles[*].url
      - data.subtitles[*].url
    (Docs advise: first fetch detail, then follow subtitle URL to get content — pattern shown for YouTube subtitles but applies similarly) 
    """
    d = detail.get("data", detail)
    urls = []
    # v1 style
    infos = (((d.get("aweme_detail") or {}).get("video") or {}).get("subtitleInfos") or [])
    urls += [x.get("url") for x in infos if isinstance(x, dict) and x.get("url")]
    # alt styles
    for key in ("video",):
        subs = (d.get(key) or {}).get("subtitles") or []
        urls += [x.get("url") for x in subs if isinstance(x, dict) and x.get("url")]
    subs = d.get("subtitles") or []
    urls += [x.get("url") for x in subs if isinstance(x, dict) and x.get("url")]
    # dedupe
    return [u for u, _ in itertools.groupby([u for u in urls if u])]

def download_url(url: str, out_path: pathlib.Path):
    _ensure_dir(out_path)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 15):
                if chunk:
                    f.write(chunk)
    return str(out_path)


def fetch_all_comments(api_client, aweme_id: str, max_pages: int = 999):
    """
    Robustly find and call whatever 'get video comments' endpoint this SDK build provides.
    Tries multiple API classes and name patterns; supports different pagination field names.
    """
    def _candidate_methods(api_obj):
        # any callable with 'comment' in the name (and ideally 'get'/'fetch')
        names = [
            n for n in dir(api_obj)
            if callable(getattr(api_obj, n)) and 'comment' in n.lower()
        ]
        # prefer getters/fetchers
        names.sort(key=lambda s: (('get' in s) or ('fetch' in s), s), reverse=True)
        for n in names:
            yield getattr(api_obj, n), n

    def _try_params(func, **base):
        # Try common param names for aweme_id / pagination
        id_keys = ("aweme_id", "video_id", "id")
        cur_keys = ("cursor", "page_cursor", "page_token", "offset", "page", "pageNo")
        # build first call (no cursor/offset yet)
        for idk in id_keys:
            try:
                kwargs = dict(base)
                kwargs[idk] = aweme_id
                # TikHub SDK wants _preload_content=False to return a stream
                kwargs.setdefault("_preload_content", False)
                resp = func(**kwargs)
                return resp, idk, cur_keys
            except TypeError:
                continue
        raise RuntimeError("Signature mismatch for comments function")

    def _parse(resp_bytes):
        data = _json(resp_bytes)
        # unify shape
        items = _first([
            data.get("comments"),
            data.get("data", {}).get("comments"),
            data.get("data", {}).get("items"),
            data.get("items"),
        ], [])
        next_cursor = _first([
            data.get("cursor"),
            data.get("data", {}).get("cursor"),
            data.get("page_cursor"),
            data.get("data", {}).get("page_cursor"),
            data.get("next_cursor"),
            data.get("data", {}).get("next_cursor"),
            data.get("offset"),
            data.get("data", {}).get("offset"),
            data.get("page_token"),
            data.get("data", {}).get("page_token"),
        ])
        has_more = bool(_first([
            data.get("has_more"),
            data.get("data", {}).get("has_more"),
            (True if next_cursor not in (None, 0, "0", "") else False) if items else False
        ], False))
        return items or [], next_cursor, has_more, data

    # Try likely API containers in order
    api_containers = []
    for cls in (
        getattr(tikhub_sdk_v2, "TikTokWebAPIApi", None),
        getattr(tikhub_sdk_v2, "TikTokAppV3APIApi", None),
        getattr(tikhub_sdk_v2, "TikTokAppAPIApi", None),
        getattr(tikhub_sdk_v2, "TikHubPublicAPIApi", None),
        getattr(tikhub_sdk_v2, "TikHubDataAPIApi", None),
    ):
        if cls:
            api_containers.append(cls(api_client))

    comments = []
    used = None

    # search for any method containing 'comment'
    for api in api_containers:
        for func, name in _candidate_methods(api):
            # quick signature probe with ID only
            try:
                resp, id_key, cur_keys = _try_params(func)
            except Exception:
                continue  # not the right method
            # parse first page
            items, cursor, has_more, _ = _parse(resp)
            if items or has_more:
                used = (api.__class__.__name__, name, id_key)
                print(f"[debug] comments via {used[0]}.{used[1]} (id param: {used[2]})")
                comments.extend(items)
                # paginate
                for _ in range(max_pages - 1):
                    if not has_more:
                        break
                    kwargs = {id_key: aweme_id, "_preload_content": False}
                    # try each cursor-style param name until one works
                    sent = False
                    for ck in cur_keys:
                        try:
                            kwargs[ck] = cursor
                            resp = func(**kwargs)
                            sent = True
                            break
                        except TypeError:
                            kwargs.pop(ck, None)
                            continue
                    if not sent:
                        break  # can't send cursor → stop
                    items, cursor, has_more, _ = _parse(resp)
                    if not items:
                        break
                    comments.extend(items)
                    time.sleep(0.2)
                return comments

    # If we reach here, no method worked
    # Helpful diagnostic: list what comment-ish methods exist at all
    found = []
    for api in api_containers:
        for _, name in _candidate_methods(api):
            found.append(f"{api.__class__.__name__}.{name}")
    raise RuntimeError("Could not locate a working comments method in SDK. "
                       f"Found methods: {', '.join(found) if found else 'none'}")


def save_captions(caption_urls, base_path: pathlib.Path):
    vtt_path = base_path.with_suffix(".captions.vtt")
    jsonl_path = base_path.with_suffix(".captions.jsonl")

    import requests, io, re, json
    def _normalize_vtt_to_jsonl(vtt_text: str):
        # tiny WebVTT → JSONL normalizer (start_ms, end_ms, text)
        out = []
        cue_re = re.compile(r"(\d{2}:\d{2}:\d{2}\.\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}\.\d{3})")
        def to_ms(hms):
            h,m,s = hms.split(":")
            sec,ms = s.split(".")
            return (int(h)*3600 + int(m)*60 + int(sec))*1000 + int(ms)
        lines = vtt_text.splitlines()
        i = 0
        while i < len(lines):
            m = cue_re.search(lines[i])
            if not m:
                i += 1
                continue
            start, end = m.group(1), m.group(2)
            i += 1
            buf = []
            while i < len(lines) and lines[i].strip():
                buf.append(lines[i])
                i += 1
            out.append({"start_ms": to_ms(start), "end_ms": to_ms(end), "text": " ".join(buf).strip()})
        return out

    # prefer SmartPlayerSubtitleRedirect first
    cap = next((u for u in caption_urls if "SmartPlayerSubtitleRedirect" in u), caption_urls[0])
    r = requests.get(cap, timeout=30)
    r.raise_for_status()
    raw = r.content
    txt = raw.decode("utf-8", errors="ignore")
    if "WEBVTT" not in txt[:16]:
        # some endpoints lie about MIME; try others in the list
        for u in caption_urls[1:]:
            r = requests.get(u, timeout=30)
            r.raise_for_status()
            raw = r.content
            txt = raw.decode("utf-8", errors="ignore")
            if "WEBVTT" in txt[:16]:
                break

    vtt_path.write_text(txt, encoding="utf-8")
    items = _normalize_vtt_to_jsonl(txt)
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
    print(f"[ok] captions → {vtt_path}, {jsonl_path}")
    return {"vtt": str(vtt_path), "jsonl": str(jsonl_path)}




# ---------- normalize key metadata ----------
def summarize_metadata(detail: Dict[str, Any]) -> Dict[str, Any]:
    d = detail.get("data", detail)
    # Common spots for fields
    aweme = _first([d.get("aweme_detail"), d.get("aweme"), d])
    desc = _first([aweme.get("desc"), d.get("desc"), d.get("description")])
    create_ts = _first([aweme.get("create_time"), d.get("create_time")])
    if isinstance(create_ts, str) and create_ts.isdigit():
        create_ts = int(create_ts)
    creator = {}
    for key in ("author", "user", "creator"):
        u = aweme.get(key) or d.get(key) or {}
        if isinstance(u, dict) and u:
            creator = u
            break

    stats = _first([aweme.get("statistics"), d.get("statistics"), {}])
    # Like/save counts vary by field name across flavors
    likes = _first([
        stats.get("digg_count"), stats.get("like_count"), d.get("like_count")
    ])
    saves = _first([
        stats.get("collect_count"), stats.get("save_count"), d.get("save_count")
    ])
    shares = _first([stats.get("share_count"), d.get("share_count")])
    comments = _first([stats.get("comment_count"), d.get("comment_count")])

    return {
        "aweme_id": _first([aweme.get("aweme_id"), d.get("aweme_id")]),
        "description": desc,
        "created_at": _utc_to_local(create_ts) if create_ts else None,
        "creator": {
            "nickname": _first([creator.get("nickname"), creator.get("unique_id")]),
            "unique_id": _first([creator.get("unique_id"), creator.get("sec_uid"), creator.get("sec_user_id")]),
            "sec_uid": _first([creator.get("sec_uid"), creator.get("sec_user_id")]),
            "uid": _first([creator.get("uid"), creator.get("id")]),
        },
        "stats": {
            "likes": likes,
            "saves": saves,
            "shares": shares,
            "comments": comments,
            "views": _first([stats.get("play_count"), d.get("play_count")]),
        },
    }

# ---------- main orchestrator ----------
def scrape_tiktok(share_url: str, out_dir="downloads"):
    root = pathlib.Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)  # ensure ./downloads exists

    with make_client() as client:
        aweme_id = resolve_aweme_id(client, share_url)
        detail = fetch_video_detail(client, aweme_id)

        # Summarize early so we can name the folder
        summary = summarize_metadata(detail)
        creator_id = (summary.get("creator") or {}).get("unique_id") or "creator"
        folder_name = _safe_filename(f"{creator_id}_{aweme_id}")
        aweme_dir = root / folder_name
        aweme_dir.mkdir(parents=True, exist_ok=True)  # per-aweme folder

        # Save raw + summary metadata
        meta_path = _ensure_dir(aweme_dir / "raw.json")
        meta_path.write_text(json.dumps(detail, ensure_ascii=False, indent=2), encoding="utf-8")

        summary_path = _ensure_dir(aweme_dir / "summary.json")
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[ok] saved metadata → {summary_path}")

        # Video URL
        play_url = extract_play_url(detail)
        if not play_url:
            play_url = fetch_highest_quality_url(client, aweme_id)
        if not play_url:
            raise RuntimeError("Couldn't find a playable URL from the API response.")

        # Download video
        video_path = aweme_dir / "video.mp4"
        print(f"[...] downloading video…")
        download_url(play_url, video_path)
        print(f"[ok] video → {video_path}")

        # Captions
        print("[...] searching for captions…")
        cap_urls = _preferred_caption_urls(detail) or _collect_caption_urls_from_detail(detail)
        cap_files = None
        if cap_urls:
            print(f"[debug] captions: {len(cap_urls)} url(s); first = {cap_urls[0][:120]}...")
            # save_captions writes <base>.vtt and <base>.jsonl; give it a clean base path
            cap_files = save_captions(cap_urls, aweme_dir / "captions")
        else:
            print("[i] no captions found in API response")

        # Comments
        print("[...] fetching comments (paginated)…")
        comments = fetch_all_comments(client, aweme_id)
        com_path = _ensure_dir(aweme_dir / "comments.jsonl")
        with open(com_path, "w", encoding="utf-8") as f:
            for c in comments:
                f.write(json.dumps(c, ensure_ascii=False) + "\n")
        print(f"[ok] {len(comments)} comments → {com_path}")

        return {
            "aweme_id": aweme_id,
            "folder": str(aweme_dir),
            "video_file": str(video_path),
            "metadata_file": str(summary_path),
            "raw_file": str(meta_path),
            "captions": cap_files,  # {"vtt": ".../captions.vtt", "jsonl": ".../captions.jsonl"} or None
            "comments_file": str(com_path),
        }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tiktok_scrape.py <tiktok_url>", file=sys.stderr)
        sys.exit(2)
    result = scrape_tiktok(sys.argv[1])
    print("\nDONE\n", json.dumps(result, indent=2, ensure_ascii=False))

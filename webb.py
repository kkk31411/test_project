# -*- coding: utf-8 -*-
import os
import re
import html
import time
import uuid
import sqlite3
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, redirect, url_for

from my_email import send_email

# ── Flask 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
app = Flask(__name__, template_folder=TEMPLATES_DIR)

RESULTS: Dict = {}

# =========================================
# theise.org 기업 테이블 크롤러
# =========================================
THEISE_URL = "https://www.theise.org/dome-company/"

THEISE_CAT_MAP = {
    "Integrated Device Manufacturer": ["Integrated Device Manufacturer", "IDM", "종합반도체"],
    "Foundry": ["Foundry", "파운드리"],
    "CAD": ["CAD", "EDA", "설계도구", "설계 자동화"],
    "CAD(해외기업 지사)": ["CAD(해외기업 지사)"],
    "Fabless": ["Fabless", "팹리스"],
    "Design house": ["Design house", "디자인하우스", "Desing house"],  # 오탈자 포함
    "IT기업": ["IT기업", "IT"],
    "SW": ["SW", "소프트웨어"],
    "기계제조및센서": ["기계제조및센서", "기계제조", "센서"],
    "반도체 유통": ["반도체 유통", "유통"],
    "부품제조": ["부품제조", "부품"],
    "제조업": ["제조업", "Manufacturing"],
    "해외기업 지사": ["해외기업 지사"],
    "해외기업 지사(유통)": ["해외기업 지사(유통)"],
    "해외기업(중국,유통)": ["해외기업(중국,유통)"],
}

def _normalize_cat(label: str) -> str:
    lab = (label or "").strip()
    for k, aliases in THEISE_CAT_MAP.items():
        if lab == k:
            return k
        if any(a.lower() == lab.lower() for a in aliases):
            return k
    return lab

def fetch_theise_table() -> List[Dict]:
    r = requests.get(THEISE_URL, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    rows: List[Dict] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue
            cat = _normalize_cat(tds[0].get_text(strip=True))
            name = tds[1].get_text(strip=True)
            a = tr.find("a")
            url = a.get("href").strip() if a and a.get("href") else ""
            rows.append({"type": cat, "name": name, "url": url})
    return rows

# =========================================
# 주소 캐시 (SQLite) + 유틸
# =========================================
CACHE_DB = os.path.join(BASE_DIR, "address_cache.sqlite")

def _ensure_cache():
    os.makedirs(BASE_DIR, exist_ok=True)
    with sqlite3.connect(CACHE_DB) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS addr_cache (
                name TEXT PRIMARY KEY,
                address TEXT,
                ts INTEGER
            )
        """)
        con.commit()

_ensure_cache()

def cache_get(name: str) -> Optional[str]:
    try:
        with sqlite3.connect(CACHE_DB) as con:
            cur = con.execute("SELECT address FROM addr_cache WHERE name=?", (name,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        return None

def cache_put(name: str, address: str):
    try:
        with sqlite3.connect(CACHE_DB) as con:
            con.execute(
                "INSERT OR REPLACE INTO addr_cache(name, address, ts) VALUES(?,?,?)",
                (name, address or "", int(time.time()))
            )
            con.commit()
    except Exception:
        pass

# =========================================
# 구글 주소 추출(지식패널/로컬) — 경량/조기종료
# =========================================
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")

def _txt(x: str) -> str:
    if not x:
        return ""
    x = html.unescape(x)
    x = re.sub(r"<[^>]+>", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def _fetch(url, params=None, timeout=4.5):
    return requests.get(url, params=params or {}, headers={"User-Agent": UA}, timeout=timeout)

# 빠른 힌트: 이름만으로 '한국 주소일 가능성' 판단 → 지역 필터 없으면 구글 조회 스킵
_BRANCH_NAME_HINTS = [
    r"\b코리아\b", r"\bKorea\b", r"\(.*Korea.*\)", r"^한국\s", r"㈜.*코리아"
]
def quick_is_korea_name(name: str) -> bool:
    n = name or ""
    return any(re.search(pat, n, flags=re.IGNORECASE) for pat in _BRANCH_NAME_HINTS)

def get_company_address_from_google(name: str) -> str:
    if not name:
        return ""
    # 1) 캐시 우선
    hit = cache_get(name)
    if hit is not None:
        return hit

    # 2) 최소 쿼리 세트 (2개) — 성공하면 즉시 종료
    queries = [
        f"{name} 주소",
        f"{name} Korea address",
    ]
    address = ""
    for q in queries:
        try:
            # 로컬(지도) 우선
            r = _fetch("https://www.google.com/search", {"q": q, "hl": "ko", "tbm": "lcl"})
            if r.status_code != 200 or not r.text:
                continue
            soup = BeautifulSoup(r.text, "html.parser")

            node = soup.select_one('[data-attrid*=":address"]')
            if node:
                address = _txt(node.get_text(" ", strip=True))
                if address:
                    break

            cand = soup.select_one("span.LrzXr, div.Io6YTe, div.wDYxhc span")
            if cand:
                address = _txt(cand.get_text(" ", strip=True))
                if address:
                    break
        except Exception:
            pass
        finally:
            # 구글에 과도한 연속요청 방지
            time.sleep(0.15)

    # 3) 캐시에 저장
    cache_put(name, address)
    return address

# =========================================
# 지역 정규화 / 해외 판정
# =========================================
REGION_KEYWORDS = {
    "서울": ["서울", "Seoul"],
    "경기": ["경기", "경기도", "Gyeonggi", "Gyeonggi-do",
           "Seongnam", "Bundang", "Pangyo", "Suwon", "Yongin", "Hwaseong",
           "Giheung", "Pyeongtaek", "Icheon", "Ansan"],
    "인천": ["인천", "Incheon", "Songdo"],
    "부산": ["부산", "Busan"],
    "대구": ["대구", "Daegu"],
    "대전": ["대전", "Daejeon"],
    "광주": ["광주", "Gwangju"],
    "울산": ["울산", "Ulsan"],
    "세종": ["세종", "Sejong"],
    "충북": ["충북", "Chungbuk", "Chungcheongbuk-do", "Cheongju"],
    "충남": ["충남", "Chungnam", "Chungcheongnam-do", "Asan", "Cheonan", "Seosan", "Dangjin"],
    "전북": ["전북", "Jeonbuk", "Jeollabuk-do", "Jeonju", "Gunsan", "Iksan"],
    "전남": ["전남", "Jeonnam", "Jeollanam-do", "Gwangyang", "Yeosu", "Suncheon"],
    "경북": ["경북", "Gyeongbuk", "Gyeongsangbuk-do", "Gumi", "Pohang"],
    "경남": ["경남", "Gyeongnam", "Gyeongsangnam-do", "Changwon", "Gimhae"],
    "제주": ["제주", "Jeju"],
}

def normalize_region_from_address(addr: str) -> str:
    a = (addr or "").lower()
    for label, keys in REGION_KEYWORDS.items():
        for k in keys:
            if k.lower() in a:
                return label
    return ""

def is_korea_address(addr: str) -> bool:
    if not addr:
        return False
    if normalize_region_from_address(addr):
        return True
    if re.search(r"\b(Korea|Republic of Korea|대한민국)\b", addr, re.IGNORECASE):
        return True
    return False

# 외국계 지사(한국) 최종 판정
def is_foreign_branch_in_korea(company_name: str, address: str) -> bool:
    return quick_is_korea_name(company_name) and is_korea_address(address)

# =========================================
# 필터 & 결과 구성 (병렬 주소 조회 + 캐시)
# =========================================
def _resolve_address_for_item(item: Dict, do_lookup: bool) -> Tuple[str, str, bool]:
    """
    반환: (region_final, address, branch_kr)
    """
    name = item["name"]

    # 주소 조회 스킵 조건일 땐 힌트 기반 빠른 판정만
    if not do_lookup:
        addr = cache_get(name) or ""
        if not addr and quick_is_korea_name(name):
            # 힌트로 한국소속만 대충 표시 (정확지역 미상)
            return ("서울" if "서울" in name else "", "", True)  # 지역라벨은 공란(또는 임의로 서울 추정 X)
        return ("", addr or "", False)

    # 실제 조회
    addr = get_company_address_from_google(name)
    if is_korea_address(addr):
        region_label = normalize_region_from_address(addr)
        region_final = region_label if region_label else "서울" if "Seoul" in addr else ""  # 지역 미상은 공란
    else:
        region_final = "해외"

    branch_kr = is_foreign_branch_in_korea(name, addr)
    return (region_final, addr, branch_kr)

def filter_and_enrich(rows: List[Dict], pick_types: List[str], per_type: int,
                      filter_regions: List[str], only_foreign_branch: bool = False) -> List[Dict]:
    wants = set(_normalize_cat(t) for t in (pick_types or []))
    grouped: Dict[str, List[Dict]] = {}
    for r in rows:
        t = _normalize_cat(r["type"])
        if wants and t not in wants:
            continue
        grouped.setdefault(t, []).append(r)

    out: List[Dict] = []
    regions_set = set(filter_regions or [])

    # 주소 조회가 반드시 필요한지 판단
    need_address_lookup = bool(regions_set) or only_foreign_branch

    # 유형별 처리
    for t, arr in grouped.items():
        # 중복 회사명 제거 (상위에 동일행이 많은 경우 속도 향상)
        seen = set()
        uniq = []
        for r in arr:
            nm = r["name"].strip()
            if nm and nm not in seen:
                uniq.append(r)
                seen.add(nm)

        # 먼저 캐시에 있는 건 즉시 사용/스킵하여 워크로드 절감
        to_resolve = []
        prebuilt: List[Tuple[Dict, Tuple[str, str, bool]]] = []

        for r in uniq:
            name = r["name"]
            cached = cache_get(name)
            if cached is not None and (not need_address_lookup or cached):
                # 캐시를 기반으로 빠르게 판정
                addr = cached
                if is_korea_address(addr):
                    reg_lab = normalize_region_from_address(addr)
                    reg_final = reg_lab if reg_lab else ""
                else:
                    reg_final = "해외" if addr else ""
                branch_kr = is_foreign_branch_in_korea(name, addr)
                prebuilt.append((r, (reg_final, addr, branch_kr)))
            else:
                to_resolve.append(r)

        # 병렬 주소 조회
        results_map: Dict[str, Tuple[str, str, bool]] = {}
        if to_resolve and need_address_lookup:
            with ThreadPoolExecutor(max_workers=min(8, max(2, os.cpu_count() or 4))) as ex:
                futs = {ex.submit(_resolve_address_for_item, r, True): r for r in to_resolve}
                for fut in as_completed(futs):
                    r = futs[fut]
                    try:
                        region_final, addr, branch_kr = fut.result()
                    except Exception:
                        region_final, addr, branch_kr = ("", "", False)
                    results_map[r["name"]] = (region_final, addr, branch_kr)

        # 조회가 꼭 필요 없으면 (필터X) 힌트 기반으로만 빠르게 채우기
        if to_resolve and not need_address_lookup:
            for r in to_resolve:
                results_map[r["name"]] = _resolve_address_for_item(r, False)

        # 합치기
        enriched: List[Dict] = []
        def accept_region(region_final: str, addr: str) -> bool:
            if not regions_set:
                return True
            # 해외 선택 시
            if region_final == "해외":
                return "해외" in regions_set
            # 국내 라벨 선택 시
            if region_final and region_final in regions_set:
                return True
            # 라벨을 못 뽑았으면 주소문자열에 지역 단서라도 있는지
            return any(reg in (addr or "") for reg in regions_set)

        # 캐시/사전 구성분
        for r, triple in prebuilt:
            region_final, addr, branch_kr = triple
            if only_foreign_branch and not branch_kr:
                continue
            if not accept_region(region_final, addr):
                continue
            enriched.append({
                "회사 유형": t,
                "회사명": r["name"],
                "지역": region_final or ("해외" if (addr and not is_korea_address(addr)) else ""),
                "주소(구글)": addr,
                "지사구분": "외국계 지사(한국)" if branch_kr else "",
                "링크": r["url"],
            })

        # 새로 조회된 것들
        for r in to_resolve:
            region_final, addr, branch_kr = results_map.get(r["name"], ("", "", False))
            if only_foreign_branch and not branch_kr:
                continue
            if not accept_region(region_final, addr):
                continue
            enriched.append({
                "회사 유형": t,
                "회사명": r["name"],
                "지역": region_final or ("해외" if (addr and not is_korea_address(addr)) else ""),
                "주소(구글)": addr,
                "지사구분": "외국계 지사(한국)" if branch_kr else "",
                "링크": r["url"],
            })

        # 유형별 상한 적용
        out.extend(enriched[:max(1, per_type)])

    return out

# =========================================
# 라우트
# =========================================
@app.route("/", methods=["GET"])
def home():
    return render_template("main.html")

# (옵션) 오른쪽 뉴스레터 폼 제출은 데모로 같은 화면 유지
@app.route("/", methods=["POST"])
def newsletter_submit():
    user_name = request.form.get("user_id")
    email = request.form.get("email")
    if not user_name or not email:
        return render_template("main.html", error="이름/이메일을 입력해주세요.")
    return render_template("main.html", error="(데모) 제출 완료! 왼쪽 기업정보 검색도 시도해보세요.")

@app.route("/company_search", methods=["POST"])
def company_search():
    categories = request.form.getlist("categories")
    per_cat = int(request.form.get("per_category") or 5)
    regions = request.form.getlist("regions")
    only_branch = (request.form.get("only_foreign_branch") == "Y")
    recipient_email = request.form.get("recipient_email")

    try:
        table = fetch_theise_table()
        rows = filter_and_enrich(table, categories, per_cat, regions, only_foreign_branch=only_branch)
    except Exception as e:
        error_msg = f"조회 실패: {e}"
        if recipient_email:
            send_email(recipient_email, "🔎 기업정보 검색 결과 - 실패", error_msg)
        return render_template("main.html", error=error_msg)

    # 이메일 본문 생성 (결과 없을 때도 발송)
    if rows:
        body_lines = ["기업정보 검색 결과:\n"]
        for r in rows:
            body_lines.append(
                f"{r['회사명']} ({r['회사 유형']})\n지역: {r['지역']}\n주소: {r['주소(구글)']}\n링크: {r['링크']}\n"
            )
    else:
        body_lines = ["조건에 해당하는 기업 정보가 없습니다. 유형/지역 조건을 조정해보세요."]
    
    body = "\n".join(body_lines)

    # 이메일 발송
    if recipient_email:
        success = send_email(recipient_email, "🔎 기업정보 검색 결과", body)
        if not success:
            print(f"⚠️ 이메일 발송 실패: {recipient_email}")


    # 결과 페이지용 저장
    job_id = uuid.uuid4().hex
    RESULTS[job_id] = {
        "filters": {
            "categories": categories or ["(전체)"],
            "per_category": per_cat,
            "regions": regions or ["(제한 없음)"],
            "only_branch": "예" if only_branch else "아니오",
        },
        "rows": rows,
        "columns": ["회사 유형", "회사명", "지역", "주소(구글)", "지사구분", "링크"],
    }
    return redirect(url_for("result_page", job_id=job_id))

@app.route("/result/<job_id>", methods=["GET"])
def result_page(job_id):
    payload = RESULTS.get(job_id)
    if not payload:
        return "결과가 만료되었거나 존재하지 않습니다.", 404
    return render_template("results.html", **payload)

if __name__ == "__main__":
    # pip install flask requests beautifulsoup4
    app.run(debug=True, host="0.0.0.0", port=5002)

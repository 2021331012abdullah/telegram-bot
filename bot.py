import os
import json
import asyncio
import datetime
import requests
import cloudscraper
from telegram import Bot, LinkPreviewOptions
import html
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from bs4 import BeautifulSoup


TOKEN = os.getenv("TOKEN")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")
SHEET_NAME = os.getenv("SHEET_NAME")
SHEET_URL = os.getenv("SHEET_URL")

cf_map = {}
vjudge_title_map = {}

def get_vjudge_problem_title(oj, prob_id):
    key = f"{oj}-{prob_id}"
    
    if key in vjudge_title_map:
        return vjudge_title_map[key]

    try:
        scraper = cloudscraper.create_scraper()
        url = f"https://vjudge.net/problem/{key}"
        resp = scraper.get(url, timeout=5)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            page_title = soup.title.string if soup.title else ""
            suffix = f" - {oj} {prob_id} - Virtual Judge"
            
            if page_title.endswith(suffix):
                clean_title = page_title[:-len(suffix)].strip()
                if clean_title:
                    vjudge_title_map[key] = clean_title
                    return clean_title
                        
    except Exception:
        pass
    
    return ""

def get_sheet_object():
    creds_dict = None
    if os.path.exists("credentials.json"):
        print("Using local credentials.json file...")
        try:
            with open("credentials.json", "r") as f:
                creds_dict = json.load(f)
        except Exception as e:
            print(f"Error reading local file: {e}")
            return None
    elif os.getenv("GCP_CREDENTIALS"):
        print("Using Environment Variable credentials...")
        try:
            creds_dict = json.loads(os.getenv("GCP_CREDENTIALS"))
        except Exception as e:
            print(f"Error parsing env var: {e}")
            return None
    else:
        print("Error: No credentials found.")
        return None

    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open_by_url(SHEET_URL).worksheet(SHEET_NAME)
    except Exception as e:
        print(f"Auth Error: {e}")
        return None


def get_codeforces_stats(handle, last_id_str):
    if not handle: return set(), set(), 0, last_id_str
    try:
        url = f"https://codeforces.com/api/user.status?handle={handle}&from=1&count=200"
        response = requests.get(url).json()
        if response['status'] != 'OK' or not response['result']: 
            return set(), set(), 0, last_id_str
        
        subs = response['result']
        current_latest_id = subs[0]['id']
        
        if not last_id_str:
            return set(), set(), 0, current_latest_id

        last_id = int(last_id_str)
        ac, wa, total = set(), set(), 0
        
        for sub in subs:
            if sub['id'] <= last_id:
                break
            
            total += 1
            try:
                p = sub['problem']
                pref = 'Gym'
                if len(str(p.get('contestId', ''))) <= 4:
                    pref = 'CodeForces' 
                name = f"{pref} {p.get('contestId', '')}{p.get('index', '')}"
                cf_map[name] = p.get('name', '')
                name = name + f" {p.get('name', '')}"
                
            except Exception as e: name = ""

            if sub.get('verdict') == 'OK': ac.add(name)
            else: wa.add(name)
        
        return ac, wa - ac, total, max(current_latest_id, last_id)

    except Exception as e: 
        print(f"CF Error {handle}: {e}")
        return set(), set(), 0, last_id_str

def get_atcoder_stats(handle, last_id_str):
    if not handle: return set(), set(), 0, last_id_str
    try:
        url = f"https://kenkoooo.com/atcoder/atcoder-api/v3/user/submissions?user={handle}&from_second=0"
        resp = requests.get(url)
        if resp.status_code != 200: return set(), set(), 0, last_id_str

        subs = resp.json()
        if not subs: return set(), set(), 0, last_id_str

        current_latest_id = max([s['id'] for s in subs])

        if not last_id_str:
            return set(), set(), 0, current_latest_id

        last_id = int(last_id_str)
        ac, wa, total = set(), set(), 0
        
        for sub in subs:
            if sub['id'] > last_id:
                total += 1
                name = f"AtCoder {sub.get('problem_id')}"
                if sub.get('result') == 'AC': ac.add(name)
                else: wa.add(name)
            
        return ac, wa - ac, total, max(current_latest_id, last_id)
    except Exception as e: 
        print(f"AC Error {handle}: {e}")
        return set(), set(), 0, last_id_str

def get_vjudge_stats(handle, last_id_str):
    if not handle: return set(), set(), 0, last_id_str
    
    ac, wa, total = set(), set(), 0
    current_latest_id = 0
    last_id = int(last_id_str) if last_id_str and str(last_id_str).isdigit() else 0
    
    start = 0
    scraper = cloudscraper.create_scraper()
    
    while True:
        try:
            params = {
                "draw": 1, "start": start, "length": 20, 
                "un": handle, "sortDir": "desc", "orderBy": "runId"
            }
            resp = scraper.get("https://vjudge.net/status/data", params=params).json()
            data = resp.get('data', [])
            
            if not data: break

            if start == 0:
                current_latest_id = int(data[0]['runId'])
                if last_id == 0:
                    return set(), set(), 0, current_latest_id

            stop_fetching = False
            for sub in data:
                run_id = int(sub['runId'])
                if run_id <= last_id:
                    stop_fetching = True
                    break
                
                total += 1
                name = f"{sub.get('oj', '')} {sub.get('probNum', '')}"

                if str(sub.get('oj', '')) == 'CodeForces' or str(sub.get('oj', '')) == 'Gym':
                    if name in cf_map and len(cf_map[name]) > 0:
                        name = name + f" {cf_map[name]}"

                if sub.get('status') == 'Accepted': ac.add(name)
                else: wa.add(name)
            
            if stop_fetching: break
            
            start += 20
            if start > 500: break 
                
        except Exception as e:
            print(f"VJ Loop Error {handle}: {e}")
            break

    new_max_id = max(current_latest_id, last_id)
    return ac, wa - ac, total, new_max_id

def get_codechef_stats(handle, last_id_str):
    if not handle: return set(), set(), 0, last_id_str
    
    ac, wa, total = set(), set(), 0
    current_latest_id = 0
    last_id = int(last_id_str) if last_id_str and str(last_id_str).isdigit() else 0
    
    page = 0
    MAX_PAGES = 40

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        while page < MAX_PAGES:
            url = f"https://www.codechef.com/recent/user?page={page}&user_handle={handle}"
            response = requests.get(url, headers=headers).json()
            
            html_content = response.get('content', '')
            if not html_content: break

            soup = BeautifulSoup(html_content, 'html.parser')
            rows = soup.select('tbody tr')
            
            if not rows: break


            if page == 0:
                try:
                    first_link = rows[0].find_all('td')[-1].find('a')['href']
                    current_latest_id = int(first_link.split('/')[-1])
                    if last_id == 0:
                        return set(), set(), 0, current_latest_id
                except:
                    return set(), set(), 0, last_id

            stop_fetching = False
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 3: continue

                try:
                    sol_link = cols[-1].find('a')['href']
                    run_id = int(sol_link.split('/')[-1])
                    
                    if run_id <= last_id:
                        stop_fetching = True
                        break
                    
                    total += 1

                    prob_link = cols[1].find('a')['href']
                    prob_code = prob_link.split('/')[-1]
                    name = f"CodeChef {prob_code}"
                    
                    verdict_col = str(cols[2]).lower()
                    is_ac = False
                    if 'tick-icon.gif' in verdict_col or 'accepted' in verdict_col:
                        is_ac = True
                    
                    if is_ac: ac.add(name)
                    else: wa.add(name)
                    
                except Exception as e:
                    continue

            if stop_fetching:
                break
                
            page += 1
            time.sleep(0.5)

    except Exception as e:
        print(f"CC Error {handle}: {e}")
        pass

    new_max_id = max(current_latest_id, last_id)
    return ac, wa - ac, total, new_max_id


async def main():
    if not TOKEN or not TARGET_CHAT_ID: return

    sheet = get_sheet_object()
    if not sheet: return

    headers = sheet.row_values(1)

    col_map = {
        'cf': headers.index('last_cf_id') + 1,
        'at': headers.index('last_at_id') + 1,
        'vj': headers.index('last_vj_id') + 1,
        'cc': headers.index('last_chef_id') + 1
    }


    users = sheet.get_all_records()
    print(f"Processing {len(users)} users...")
    
    results = []

    for i, user in enumerate(users):
        row_num = i + 2
        name = user.get('name', '')
        print(f"Checking {name} (Row {row_num})...")
        
        final_ac, final_wa, final_total = set(), set(), 0
        
        # CodeForces
        prev_cf = str(user.get('last_cf_id', '')).strip()
        ac, wa, tot, new_cf = get_codeforces_stats(user.get('cf_handle'), prev_cf)
        final_ac.update(ac); final_wa.update(wa); final_total += tot

        # AtCoder
        prev_at = str(user.get('last_at_id', '')).strip()
        ac, wa, tot, new_at = get_atcoder_stats(user.get('atcoder_handle'), prev_at)
        final_ac.update(ac); final_wa.update(wa); final_total += tot
        
        # VJudge
        prev_vj = str(user.get('last_vj_id', '')).strip()
        ac, wa, tot, new_vj = get_vjudge_stats(user.get('vjudge_handle'), prev_vj)
        final_ac.update(ac); final_wa.update(wa); final_total += tot

        # CodeChef
        prev_cc = str(user.get('last_chef_id', '')).strip()
        ac, wa, tot, new_cc = get_codechef_stats(user.get('codechef_handle'), prev_cc)
        final_ac.update(ac); final_wa.update(wa); final_total += tot

        while True:
            try:
                if str(new_cf) != prev_cf: sheet.update_cell(row_num, col_map['cf'], str(new_cf))
                if str(new_at) != prev_at: sheet.update_cell(row_num, col_map['at'], str(new_at))
                if str(new_vj) != prev_vj: sheet.update_cell(row_num, col_map['vj'], str(new_vj))
                if str(new_cc) != prev_cc: sheet.update_cell(row_num, col_map['cc'], str(new_cc))
                
                time.sleep(1.5)
                break
            except Exception as e:
                print(f"⚠️ Write failed for {name}: {e}. Retrying...")
                time.sleep(5)
                continue

        final_wa = final_wa - final_ac
        
        results.append({
            "name": name,
            "reg": user.get('reg_num', 'N/A'),
            "ac_list": sorted(list(final_ac)),
            "wa_list": sorted(list(final_wa)),
            "ac_count": len(final_ac),
            "wa_count": len(final_wa),
            "total": final_total
        })

    results.sort(key=lambda u: (-u['ac_count'], u['wa_count'], u['total'], -u['reg']))
    
    bot = Bot(token=TOKEN)
    header_date = html.escape(str(datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=6))).strftime("%B %d, %Y, %I:%M %p")))
    current_chunk = f"🏆 <b>Daily CP Update</b> 🏆\n  {header_date}\n\n"
    MAX_LENGTH = 4000 

    async def send_chunk(text):
        try: await bot.send_message(chat_id=TARGET_CHAT_ID, text=text, parse_mode="HTML", link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e: print(f"Telegram Error: {e}")

    async def append_or_send(new_text):
        nonlocal current_chunk
        if len(current_chunk) + len(new_text) > MAX_LENGTH:
            await send_chunk(current_chunk)
            current_chunk = ""
        current_chunk += new_text
    
    def make_vjudge_link(prob_str):
        parts = prob_str.split()
        if len(parts) >= 2:
            oj = parts[0]
            pid = parts[1]
            
            if len(parts) == 2:
                title = get_vjudge_problem_title(oj, pid)
                if title:
                    prob_str = f"{prob_str} {title}"
            
            url = f"https://vjudge.net/problem/{oj}-{pid}/origin"
            safe_text = html.escape(prob_str)
            return f'<a href="{url}">{safe_text}</a>'
        else:
            return html.escape(prob_str)

    rank = 1
    for r in results:
        if rank == 1: icon = "🥇"
        elif rank == 2: icon = "🥈"
        elif rank == 3: icon = "🥉"
        else: icon = f"<b>{rank}.</b>"
        
        safe_name = html.escape(str(r['name']))
        safe_reg = html.escape(str(r['reg']))
        
        buffer = f"{icon} <b>{safe_name}</b> ({safe_reg})\n"
        buffer += (f"  ✅ <b>AC:</b>{r['ac_count']}"
                   f"  ❌ <b>WA:</b>{r['wa_count']}"
                   f"  ❔<b>SUB:</b>{r['total']}\n")
        await append_or_send(buffer)

        has_activity = False
        
        for prob in r['ac_list']:
            link = make_vjudge_link(prob)
            await append_or_send(f"\u3000☑️ {link}\n")
            has_activity = True
            
        for prob in r['wa_list']:
            link = make_vjudge_link(prob)
            await append_or_send(f"\u3000⁉️ {link}\n")
            has_activity = True
            
        if not has_activity:
            await append_or_send("    💤 <i>No activity</i>\n")
        
        await append_or_send("\n\n")
        rank += 1

    if current_chunk.strip():
        await send_chunk(current_chunk)
    
    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
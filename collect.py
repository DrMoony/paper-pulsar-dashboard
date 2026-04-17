"""Dashboard data collector — runs every 30 min via LaunchAgent.

Outputs: dashboard/data.json with codex batches, daily pipeline stats, Firestore counts.
"""
import json, os, sys, re, subprocess, glob
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, '/Users/moonimoo/paper-pulsar/backend')
import firebase_admin
from firebase_admin import credentials, firestore

if not firebase_admin._apps:
    firebase_admin.initialize_app(credentials.Certificate('/Users/moonimoo/paper-pulsar/backend/service-account-key.json'))
db = firestore.client()

KST = timezone(timedelta(hours=9))
NOW_KST = datetime.now(KST)


def collect_codex_batches():
    batches = []
    for d in sorted(glob.glob('/Users/moonimoo/codex_work/v4_remaining_b*')):
        name = os.path.basename(d).replace('v4_remaining_', '').upper()
        try:
            with open(f'{d}/input.json') as f:
                total = len(json.load(f))
        except Exception:
            continue
        done = len(glob.glob(f'{d}/output_files/*.json'))
        log = ''
        try:
            with open(f'{d}/run.log') as f:
                log = f.read()
        except Exception:
            pass
        # Parse latest progress line: [HH:MM:SS] R{N} [DONE/TOTAL] X.Xs/p, ~Ymin left, Z err
        last_match = None
        for line in log.split('\n'):
            m = re.search(r'\[(\d+:\d+:\d+)\]\s+R\d+\s+\[(\d+)/(\d+)\]\s+([\d.]+)s/p,\s+~(\d+)min\s+left,\s+(\d+)\s+err', line)
            if m: last_match = m
        speed = float(last_match.group(4)) if last_match else None
        eta_min = int(last_match.group(5)) if last_match else None
        errors = int(last_match.group(6)) if last_match else 0
        # Also count timeouts in log
        timeouts = log.count('TIMEOUT:')
        # Active = process still running with this batch path?
        ps = subprocess.run(['pgrep','-f',f'run_remaining.py {name[1:]}$'], capture_output=True, text=True)
        active = bool(ps.stdout.strip())
        batches.append({
            'name': name, 'total': total, 'done': done,
            'pct': round(done/total*100, 1) if total else 0,
            'speed_sec': speed, 'eta_min': eta_min,
            'errors': errors, 'timeouts': timeouts,
            'active': active,
        })
    return batches


def collect_firestore_status():
    status_counts = {}
    for s in ['analyzed','pending','queued_codex','metadata','skipped','failed','summarizing']:
        try:
            n = db.collection('papers').where('status','==',s).count().get()[0][0].value
            status_counts[s] = int(n)
        except Exception as e:
            status_counts[s] = -1
    cs_counts = {}
    for cs in ['retag-flash','mlx-v53','mlx-v52']:
        try:
            n = db.collection('papers').where('categorySource','==',cs).count().get()[0][0].value
            cs_counts[cs] = int(n)
        except Exception:
            cs_counts[cs] = -1
    review = -1
    try:
        review = int(db.collection('papers').where('categoryReviewNeeded','==',True).count().get()[0][0].value)
    except Exception:
        pass
    return {'status': status_counts, 'categorySource': cs_counts, 'reviewNeeded': review}


def collect_daily_pipeline():
    """Parse uvicorn.log for last analyze_papers run summary."""
    log_path = '/Users/moonimoo/paper-pulsar/uvicorn.log'
    runs = []
    try:
        with open(log_path) as f:
            for line in f:
                m = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*analyze_papers DONE: analyzed=(\d+)\s+skipped=(\d+)\s+retried=(\d+)\s+failed=(\d+)', line)
                if m:
                    runs.append({
                        'time': m.group(1),
                        'analyzed': int(m.group(2)),
                        'skipped': int(m.group(3)),
                        'retried': int(m.group(4)),
                        'failed': int(m.group(5)),
                    })
                # legacy: only analyzed/failed
                m2 = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*analyze_papers DONE: analyzed=(\d+),\s+failed=(\d+)', line)
                if m2 and not m:
                    runs.append({'time': m2.group(1), 'analyzed': int(m2.group(2)), 'failed': int(m2.group(3))})
    except Exception:
        pass
    return runs[-10:]  # last 10 runs


def collect_system():
    info = {}
    try:
        df = subprocess.run(['df','-h','/'], capture_output=True, text=True).stdout.split('\n')[1].split()
        info['disk'] = {'total': df[1], 'used': df[2], 'avail': df[3], 'pct': df[4]}
    except Exception:
        pass
    try:
        info['mlx_lm_running'] = bool(subprocess.run(['pgrep','-f','mlx_lm.server'], capture_output=True, text=True).stdout.strip())
        info['uvicorn_running'] = bool(subprocess.run(['pgrep','-f','uvicorn main:app'], capture_output=True, text=True).stdout.strip())
    except Exception:
        pass
    return info


data = {
    'updated_at_kst': NOW_KST.strftime('%Y-%m-%d %H:%M:%S KST'),
    'updated_at_iso': NOW_KST.isoformat(),
    'codex_batches': collect_codex_batches(),
    'firestore': collect_firestore_status(),
    'daily_runs': collect_daily_pipeline(),
    'system': collect_system(),
}

out_path = '/Users/moonimoo/paper-pulsar/dashboard/data.json'
with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
print(f'[{NOW_KST.strftime("%H:%M:%S")}] data.json updated ({os.path.getsize(out_path)} bytes)')

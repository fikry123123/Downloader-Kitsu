import gazu
import requests
import os
import getpass
import sys
import time
import json
from datetime import datetime

KITSU_HOST = "" 
CACHE_FILENAME = "kitsu_scan_cache.json"

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def sanitize(name):
    """Membersihkan nama file/folder"""
    if not name: return "Unnamed"
    return "".join([c for c in name if c.isalnum() or c in (' ', '.', '_', '-')]).strip()

def get_full_url(raw_url):
    """Memperbaiki URL relatif"""
    if not raw_url: return None
    if raw_url.startswith("http"): return raw_url
    
    base = KITSU_HOST.replace("/api", "")
    if raw_url.startswith("/"): return f"{base}{raw_url}"
    return f"{base}/{raw_url}"

def format_bytes(size):
    if not size: return "0 B"
    try:
        size = int(size)
        power = 2**10
        n = size
        power_labels = {0 : '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
        count = 0
        while n > power:
            n /= power
            count += 1
        return f"{n:.2f} {power_labels.get(count, 'B')}"
    except: return "Unknown"

# ==========================================
# CACHE FILE HANDLERS
# ==========================================

def save_cache_to_disk(cache_data):
    """Menyimpan data hasil scan ke file JSON"""
    try:
        data_wrapper = {
            "timestamp": time.time(),
            "date_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": cache_data
        }
        with open(CACHE_FILENAME, 'w') as f:
            json.dump(data_wrapper, f, indent=2)
        print(f"\n[INFO] Data scan berhasil disimpan ke '{CACHE_FILENAME}'")
    except Exception as e:
        print(f"[WARNING] Gagal menyimpan cache: {e}")

def load_cache_from_disk():
    """Membaca file cache jika ada"""
    if not os.path.exists(CACHE_FILENAME):
        return None
    
    try:
        with open(CACHE_FILENAME, 'r') as f:
            wrapper = json.load(f)
            
        raw_data = wrapper.get("data", {})
        processed_data = {}
        for k, v in raw_data.items():
            processed_data[int(k)] = v
            
        return processed_data, wrapper.get("date_str", "Unknown Date")
    except Exception as e:
        print(f"[WARNING] File cache rusak atau tidak valid: {e}")
        return None

# ==========================================
# DOWNLOAD URL LOGIC
# ==========================================

def generate_url_candidates(entity_type, entity_id):
    base_no_api = KITSU_HOST.replace("/api", "")
    base_api = KITSU_HOST
    
    candidates = []
    if entity_type == 'preview':
        candidates.append(f"{base_api}/movies/originals/preview-files/{entity_id}/download")
        candidates.append(f"{base_api}/pictures/originals/preview-files/{entity_id}/download")
        candidates.append(f"{base_api}/movies/preview-files/{entity_id}/download")
        candidates.append(f"{base_api}/pictures/preview-files/{entity_id}/download")
        candidates.append(f"{base_no_api}/api/movies/originals/preview-files/{entity_id}/download")
        candidates.append(f"{base_no_api}/api/pictures/originals/preview-files/{entity_id}/download")
        
    elif entity_type == 'output':
        candidates.append(f"{base_api}/data/output-files/{entity_id}/download")
        candidates.append(f"{base_api}/data/output-files/{entity_id}/file")
        
    elif entity_type == 'working':
        candidates.append(f"{base_api}/data/working-files/{entity_id}/download")
        candidates.append(f"{base_api}/data/working-files/{entity_id}/file")
    
    return candidates

def download_with_auto_fix(item, headers):
    folder = item['folder']
    filename = item['filename']
    
    try:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        if not os.access(folder, os.W_OK):
            return False
    except Exception as e:
        return False
    
    filepath = os.path.join(folder, filename)
    temp_filepath = filepath + ".tmp"
    
    if os.path.exists(filepath):
        file_size = os.path.getsize(filepath)
        expected_size = item.get('size', 0)
        if file_size > 1000000 or (expected_size > 0 and file_size >= expected_size * 0.95):
            return True
        elif file_size < 1000000:
            try: os.remove(filepath)
            except: pass

    url_list = []
    if item.get('url'): url_list.append(item['url'])
    url_list.extend(generate_url_candidates(item['type'], item['id']))

    for url in url_list:
        for attempt in range(3):
            try:
                timeout = (30, 600)
                with requests.get(url, headers=headers, stream=True, timeout=timeout, allow_redirects=True) as r:
                    if r.status_code in [404, 403]: break 
                    r.raise_for_status()
                    
                    content_length = r.headers.get('content-length')
                    expected_bytes = int(content_length) if content_length else 0
                    
                    with open(temp_filepath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=524288):
                            if chunk: f.write(chunk)
                    
                    temp_size = os.path.getsize(temp_filepath)
                    is_valid = False
                    
                    if expected_bytes > 0:
                        is_valid = temp_size >= expected_bytes * 0.95
                    elif temp_size > 1000000: is_valid = True
                    elif temp_size > 100000: is_valid = True
                    
                    if is_valid:
                        if os.path.exists(filepath):
                            try: os.remove(filepath)
                            except: pass
                        os.rename(temp_filepath, filepath)
                        return True
                    else:
                        if os.path.exists(temp_filepath): os.remove(temp_filepath)
                        if attempt < 2: time.sleep(2)
                        continue
                        
            except Exception:
                if os.path.exists(temp_filepath):
                    try: os.remove(temp_filepath)
                    except: pass
                if attempt < 2:
                    time.sleep(2)
                    continue
                break
    
    if os.path.exists(temp_filepath):
        try: os.remove(temp_filepath)
        except: pass
    
    return False

# ==========================================
# SCANNING & MAPPING LOGIC
# ==========================================
PARENT_NAME_CACHE = {}

def get_parent_name_direct(parent_id, headers):
    if not parent_id: return "No_Parent"
    if parent_id in PARENT_NAME_CACHE: return PARENT_NAME_CACHE[parent_id]
    try:
        url = f"{KITSU_HOST}/data/entities/{parent_id}"
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            name = r.json().get('name', 'Unknown_Parent')
            PARENT_NAME_CACHE[parent_id] = name 
            return name
    except: pass
    return f"Parent_{parent_id[:8]}" 

def resolve_sequence_name(entity, seq_map, headers):
    if entity.get('sequence_name'): return entity['sequence_name']
    seq_id = entity.get('sequence_id')
    if seq_id and seq_id in seq_map: return seq_map[seq_id]
    parent_id = entity.get('parent_id')
    if parent_id and parent_id in seq_map: return seq_map[parent_id]
    if parent_id: return get_parent_name_direct(parent_id, headers)
    return "No_Sequence"

def normalize_list_response(payload):
    if isinstance(payload, dict) and 'data' in payload: payload = payload['data']
    return payload if isinstance(payload, list) else []

def get_episodes_for_project(project, headers):
    if hasattr(gazu, "episode") and hasattr(gazu.episode, "all_episodes_for_project"):
        try: return gazu.episode.all_episodes_for_project(project)
        except: pass
    try:
        url = f"{KITSU_HOST}/data/episodes?project_id={project['id']}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200: return normalize_list_response(r.json())
    except: pass
    return []

def get_sequences_for_episode(episode, headers):
    if hasattr(gazu, "sequence") and hasattr(gazu.sequence, "all_sequences_for_episode"):
        try: return gazu.sequence.all_sequences_for_episode(episode)
        except: pass
    try:
        url = f"{KITSU_HOST}/data/sequences?episode_id={episode['id']}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200: return normalize_list_response(r.json())
    except: pass
    return []

def get_sequences_for_project(project, headers):
    if hasattr(gazu, "sequence") and hasattr(gazu.sequence, "all_sequences_for_project"):
        try: return gazu.sequence.all_sequences_for_project(project)
        except: pass
    try:
        url = f"{KITSU_HOST}/data/sequences?project_id={project['id']}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200: return normalize_list_response(r.json())
    except: pass
    return []

def resolve_episode_and_sequence(entity, seq_map, seq_episode_map, episode_map, headers):
    seq_name_raw = resolve_sequence_name(entity, seq_map, headers)
    seq_name = sanitize(seq_name_raw)
    episode_name = "No_Episode"

    seq_id = entity.get('sequence_id') or entity.get('parent_id')
    ep_id = entity.get('episode_id')

    if not ep_id and seq_id and seq_id in seq_episode_map:
        ep_id = seq_episode_map[seq_id]
    if ep_id and ep_id in episode_map:
        episode_name = episode_map[ep_id]
    elif ep_id:
        episode_name = get_parent_name_direct(ep_id, headers)

    return sanitize(episode_name), seq_name

def scan_entity(entity, root_folder, entity_type, download_queue, seq_map, episode_map, seq_episode_map, headers):
    entity_name = sanitize(entity['name'])
    
    if entity_type == 'Shot':
        episode_name, seq_name = resolve_episode_and_sequence(
            entity, seq_map, seq_episode_map, episode_map, headers
        )
        base_folder = os.path.join(root_folder, episode_name, seq_name, entity_name)
    else:
        type_name = sanitize(entity.get('asset_type_name', 'Props'))
        base_folder = os.path.join(root_folder, "Assets", type_name, entity_name)

    # Preview
    if entity.get('preview_file_id'):
        try:
            pf = gazu.files.get_preview_file(entity['preview_file_id'])
            if pf:
                base_name = pf.get('original_name') or pf.get('name') or entity_name
                ext = pf.get('extension', 'mp4')
                clean_name = sanitize(base_name)
                if not clean_name.lower().endswith(f".{ext}"): clean_name = f"{clean_name}.{ext}"
                download_queue.append({
                    'type': 'preview', 'id': pf['id'], 'url': get_full_url(pf.get('url')),
                    'folder': base_folder, 'filename': clean_name, 'size': pf.get('file_size', 0)
                })
        except: pass

    # Tasks
    try:
        if entity_type == 'Shot': tasks = gazu.task.all_tasks_for_shot(entity)
        else: tasks = gazu.task.all_tasks_for_asset(entity)

        for task in tasks:
            task_type = sanitize(task['task_type_name'])
            task_folder = os.path.join(base_folder, task_type)
            task_files = []

            if task.get('preview_file_id'):
                try:
                    pf = gazu.files.get_preview_file(task['preview_file_id'])
                    if pf:
                        base_name = pf.get('original_name') or pf.get('name')
                        ext = pf.get('extension', 'mp4')
                        clean_name = f"{task_type}_Preview_{sanitize(base_name)}.{ext}"
                        task_files.append({
                            'type': 'preview', 'id': pf['id'], 'url': get_full_url(pf.get('url')),
                            'folder': task_folder, 'filename': clean_name, 'size': pf.get('file_size', 0)
                        })
                except: pass

            outputs = gazu.files.all_output_files_for_entity(task)
            for out in outputs:
                base_name = out.get('original_name') or out.get('name')
                ext = out.get('extension', '')
                clean_name = f"{task_type}_Output_{sanitize(base_name)}"
                if ext and not clean_name.lower().endswith(f".{ext}"): clean_name = f"{clean_name}.{ext}"
                task_files.append({
                    'type': 'output', 'id': out['id'], 'url': get_full_url(out.get('url')),
                    'folder': task_folder, 'filename': clean_name, 'size': out.get('file_size', 0)
                })

            works = gazu.files.all_working_files_for_entity(task)
            for work in works:
                base_name = work.get('original_name') or work.get('name')
                ext = work.get('extension', '')
                clean_name = f"{task_type}_SRC_{sanitize(base_name)}"
                if ext and not clean_name.lower().endswith(f".{ext}"): clean_name = f"{clean_name}.{ext}"
                task_files.append({
                    'type': 'working', 'id': work['id'], 'url': get_full_url(work.get('url')),
                    'folder': task_folder, 'filename': clean_name, 'size': work.get('file_size', 0)
                })

            if task_files: download_queue.extend(task_files)
    except Exception: pass

def analyze_single_project(project, auth_headers, proj_idx, total_projects):
    """Scan satu project dan return (total_size, total_files, download_queue, root, total_shots)"""
    home_dir = os.path.expanduser("~")
    downloads_path = os.path.join(home_dir, "Downloads")
    folder_name = f"Kitsu_{sanitize(project['name'])}"
    download_root = os.path.join(downloads_path, folder_name)

    seq_map = {}
    episode_map = {}
    seq_episode_map = {}
    
    try:
        episodes = get_episodes_for_project(project, auth_headers)
        for ep in episodes:
            episode_map[ep['id']] = ep['name']
            seqs = get_sequences_for_episode(ep, auth_headers)
            for s in seqs:
                seq_map[s['id']] = s['name']
                seq_episode_map[s['id']] = ep['id']
        
        root_seqs = get_sequences_for_project(project, auth_headers)
        for s in root_seqs:
            seq_map[s['id']] = s['name']
            ep_id = s.get('episode_id')
            if ep_id:
                seq_episode_map[s['id']] = ep_id
                if ep_id not in episode_map:
                    episode_map[ep_id] = get_parent_name_direct(ep_id, auth_headers)
    except: pass

    download_queue = []
    
    # === MODIFIKASI: MENGHITUNG SHOT SECARA EKSPLISIT ===
    shots = gazu.shot.all_shots_for_project(project)
    shot_count = len(shots) # Menghitung jumlah shot
    
    assets = gazu.asset.all_assets_for_project(project)
    
    total_items = len(shots) + len(assets)
    processed_count = 0
    
    def print_scan_progress():
        nonlocal processed_count
        percent = int((processed_count / total_items) * 100) if total_items > 0 else 100
        sys.stdout.write(f"\r>> [{proj_idx}/{total_projects}] Menganalisis '{project['name']}' ... [{percent:>3}%] ({processed_count}/{total_items} items)")
        sys.stdout.flush()
    
    print_scan_progress()

    for shot in shots:
        processed_count += 1
        print_scan_progress()
        scan_entity(shot, download_root, 'Shot', download_queue, seq_map, episode_map, seq_episode_map, auth_headers)

    if assets:
        for asset in assets:
            processed_count += 1
            print_scan_progress()
            scan_entity(asset, download_root, 'Asset', download_queue, seq_map, episode_map, seq_episode_map, auth_headers)
            
    total_size = sum(item.get('size', 0) for item in download_queue)
    sys.stdout.write(f"\r>> [{proj_idx}/{total_projects}] Menganalisis '{project['name']}' ... [DONE] Found {len(download_queue)} files ({format_bytes(total_size)})   \n")
    sys.stdout.flush()
    
    # Mengembalikan shot_count juga
    return total_size, len(download_queue), download_queue, download_root, shot_count

# ==========================================
# MAIN
# ==========================================

def main():
    global KITSU_HOST 
    print("="*60)
    print("   KITSU DOWNLOADER - SMART CACHE MODE")
    print("="*60)

    # --- 1. SETUP KONEKSI ---
    while True:
        try:
            if not KITSU_HOST:
                host_input = input("URL Server Kitsu (cth: http://192.168.1.50/api): ").strip()
                if not host_input: continue
                if not host_input.endswith("/api"): host_input = f"{host_input}/api"
                KITSU_HOST = host_input

            print(f"Target Server: {KITSU_HOST}")
            user_input = input("Username/Email : ")
            pass_input = getpass.getpass("Password       : ")
            
            gazu.client.set_host(KITSU_HOST)
            gazu.log_in(user_input, pass_input)
            print(">> Login BERHASIL!\n")
            break
        except Exception as e:
            print(f"!! Login GAGAL: {e}\n")
            KITSU_HOST = "" 

    # --- 2. AMBIL TOKEN ---
    auth_headers = None
    try:
        client_instance = gazu.client.default_client
        raw_tokens = client_instance.tokens
        auth_headers = {"Authorization": f"Bearer {raw_tokens['access_token']}"}
    except Exception as e:
        print(f"[X] Gagal ambil token: {e}")
        return

    # --- 3. GET PROJECT LIST ---
    try:
        all_projects = gazu.project.all_open_projects()
    except:
        print("Gagal ambil project."); return

    if not all_projects:
        print("Tidak ada project aktif."); return

    # --- 4. CEK CACHE ATAU SCAN BARU ---
    PROJECT_CACHE = {} 
    
    # Cek apakah ada file cache
    loaded_cache_data, cache_date = load_cache_from_disk() or (None, None)
    use_cache = False

    if loaded_cache_data:
        print("="*60)
        print(f"DITEMUKAN DATA SCAN TERSIMPAN!")
        print(f"Tanggal Scan: {cache_date}")
        print("="*60)
        confirm_cache = input(">> Gunakan data lama (tidak perlu scan ulang)? (y/n): ").lower().strip()
        if confirm_cache == 'y':
            use_cache = True
            PROJECT_CACHE = loaded_cache_data
            print("\n>> Data berhasil dimuat dari file.")
        else:
            print(">> Melakukan scan ulang...")

    if not use_cache:
        print("="*60)
        print(f">> MEMULAI ANALISIS {len(all_projects)} PROJECT...")
        print("   (Harap tunggu, data akan disimpan setelah selesai)")
        print("="*60)

        for idx, proj in enumerate(all_projects):
            try:
                # Menangkap 5 variable return (ada p_shots)
                p_size, p_files, p_queue, p_root, p_shots = analyze_single_project(proj, auth_headers, idx+1, len(all_projects))
                PROJECT_CACHE[idx] = {
                    'project': proj, 
                    'total_size': p_size,
                    'total_files': p_files,
                    'queue': p_queue,
                    'download_root': p_root,
                    'total_shots': p_shots # Simpan jumlah shot ke cache
                }
            except Exception as e:
                print(f"\n[X] Error analyzing {proj['name']}: {e}")
                PROJECT_CACHE[idx] = None 
        
        # Simpan ke file setelah scan selesai
        save_cache_to_disk(PROJECT_CACHE)

    # --- 5. LOOP MENU UTAMA ---
    while True:
        print("\n" + "="*80) # Lebarkan sedikit agar muat
        print("   DAFTAR PROJECT")
        print("="*80)
        
        for idx, proj in enumerate(all_projects):
            data = PROJECT_CACHE.get(idx)
            
            if not data and use_cache:
                print(f"[{idx+1}] {proj['name']:<25} | (Data tidak ada di cache - Rescan diperlukan)")
                continue

            if data:
                size_str = format_bytes(data['total_size'])
                files_count = data['total_files']
                # Ambil total shot (fallback ke '-' jika cache lama belum ada datanya)
                shots_count = data.get('total_shots', '-') 
                
                print(f"[{idx+1}] {proj['name']:<25} | Size: {size_str:<9} | Files: {files_count:<5} | Shots: {shots_count}")
            else:
                print(f"[{idx+1}] {proj['name']:<25} | (Gagal Analisis)")

        print("-" * 80)
        
        # --- PILIH PROJECT ---
        selected_data = None
        while True:
            choice = input("Pilih nomor project (atau 'x' untuk keluar aplikasi): ").strip()
            if choice.lower() == 'x': 
                print("Exiting...")
                return
            
            try:
                idx = int(choice) - 1
                if idx in PROJECT_CACHE and PROJECT_CACHE[idx] is not None:
                    selected_data = PROJECT_CACHE[idx]
                    break
                else:
                    print("Nomor tidak valid atau data project kosong.")
            except ValueError:
                print("Masukkan angka yang benar.")

        # --- KONFIRMASI DOWNLOAD ---
        final_queue = selected_data['queue']
        final_root = selected_data['download_root']
        final_size = selected_data['total_size']
        human_size = format_bytes(final_size)
        total_shots = selected_data.get('total_shots', 'Unknown')
        
        p_name = selected_data['project']['name'] if isinstance(selected_data['project'], dict) else selected_data['project']['name']

        if not final_queue:
            print(f"\n!! Project '{p_name}' kosong. Kembali ke menu...")
            time.sleep(1)
            continue

        print("\n" + "="*60)
        print(f"   DETAIL DOWNLOAD: {p_name}")
        print(f"   Lokasi      : {final_root}")
        print(f"   Total Shots : {total_shots}")
        print(f"   Total Files : {len(final_queue)} files")
        print(f"   Total Size  : {human_size}")
        print("="*60)
        
        confirm = input(">> Mulai download sekarang? (y/n): ").lower().strip()
        
        if confirm != 'y':
            print(">> Kembali ke menu utama...")
            time.sleep(0.5)
            continue 

        # --- EKSEKUSI DOWNLOAD ---
        print("\n>> Memulai Download...")
        try: os.makedirs(final_root, exist_ok=True)
        except: pass
        
        success_count = 0
        failed_count = 0
        downloaded_size = 0
        start_time = time.time()
        total_files = len(final_queue)

        for i, item in enumerate(final_queue):
            percent = int((i + 1) / total_files * 100)
            fname = item['filename']
            file_size_display = format_bytes(item.get('size', 0))
            if len(fname) > 30: fname = fname[:27] + "..."
            
            sys.stdout.write(f"\r[{percent}%] {fname:<33} ({file_size_display})")
            sys.stdout.flush()
            
            if download_with_auto_fix(item, headers=auth_headers):
                success_count += 1
                downloaded_size += item.get('size', 0)
            else:
                failed_count += 1

        duration = time.time() - start_time
        
        print(f"\n\n" + "="*60)
        print(f"SELESAI DALAM {duration:.1f} DETIK")
        print(f"Sukses   : {success_count} file")
        print(f"Gagal    : {failed_count} file")
        print(f"Folder   : {final_root}")
        print("="*60)

        retry = input("\n>> Apakah Anda ingin download project lain? (y/n): ").lower().strip()
        if retry == 'y': continue 
        else:
            print("Terima kasih. Keluar aplikasi.")
            break 

if __name__ == "__main__":
    main()
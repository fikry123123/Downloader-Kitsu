import gazu
import requests
import os
import getpass
import sys
import time
import json

KITSU_HOST = "" 


def sanitize(name):
    """Membersihkan nama file/folder"""
    if not name: return "Unnamed"
    return "".join([c for c in name if c.isalnum() or c in (' ', '.', '_', '-')]).strip()

def get_full_url(raw_url):
    """Memperbaiki URL relatif"""
    if not raw_url: return None
    if raw_url.startswith("http"): return raw_url
    
    # Menggunakan global variable yang sudah diinput user
    base = KITSU_HOST.replace("/api", "")
    if raw_url.startswith("/"): return f"{base}{raw_url}"
    return f"{base}/{raw_url}"

def format_bytes(size):
    if not size: return "Unknown"
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

def generate_url_candidates(entity_type, entity_id):
    """Membuat berbagai variasi URL untuk download file"""
    base_no_api = KITSU_HOST.replace("/api", "")
    base_api = KITSU_HOST
    
    candidates = []
    if entity_type == 'preview':
        # Try both pictures dan movies endpoint (Kitsu menyimpan preview di kedua tempat)
        candidates.append(f"{base_api}/movies/originals/preview-files/{entity_id}/download")
        candidates.append(f"{base_api}/pictures/originals/preview-files/{entity_id}/download")
        candidates.append(f"{base_api}/movies/preview-files/{entity_id}/download")
        candidates.append(f"{base_api}/pictures/preview-files/{entity_id}/download")
        # Fallback variants
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
    """Mencoba download dengan multiple URL candidates dan retry logic"""
    folder = item['folder']
    filename = item['filename']
    
    # Buat folder dengan error handling yang lebih baik
    try:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        
        # Cek write permission
        if not os.access(folder, os.W_OK):
            return False
    except Exception as e:
        return False
    
    filepath = os.path.join(folder, filename)
    temp_filepath = filepath + ".tmp"
    
    # Cek file yang sudah lengkap - validasi dengan expected size
    if os.path.exists(filepath):
        file_size = os.path.getsize(filepath)
        expected_size = item.get('size', 0)
        
        # Jika file sudah ada dan size match atau cukup besar (min 1MB untuk video)
        if file_size > 1000000 or (expected_size > 0 and file_size >= expected_size * 0.95):
            return True
        # Jika file ada tapi kecil (< 1MB), hapus dan download ulang
        elif file_size < 1000000:
            try:
                os.remove(filepath)
            except:
                pass

    url_list = []
    if item.get('url'): url_list.append(item['url'])
    url_list.extend(generate_url_candidates(item['type'], item['id']))

    # Retry logic - coba setiap URL hingga 3 kali
    for url in url_list:
        for attempt in range(3):
            try:
                # Timeout lebih panjang untuk file video besar
                timeout = (30, 600)  # (connect, read) timeout - 10 menit untuk file besar
                
                with requests.get(url, headers=headers, stream=True, timeout=timeout, allow_redirects=True) as r:
                    if r.status_code == 404: 
                        break  # URL tidak ada, coba URL berikutnya
                    if r.status_code == 403:
                        break  # Forbidden, skip URL ini
                    r.raise_for_status()
                    
                    # Get content length dari header
                    content_length = r.headers.get('content-length')
                    expected_bytes = int(content_length) if content_length else 0
                    
                    # Download ke file temporary dulu
                    with open(temp_filepath, 'wb') as f:
                        total_downloaded = 0
                        for chunk in r.iter_content(chunk_size=524288):  # 512KB chunks
                            if chunk:
                                f.write(chunk)
                                total_downloaded += len(chunk)
                    
                    # Validasi ukuran file yang didownload
                    temp_size = os.path.getsize(temp_filepath)
                    
                    # Cek valid: minimal 1MB untuk video, atau match dengan expected
                    min_valid_size = 1000000  # 1MB minimum untuk file
                    is_valid = False
                    
                    if expected_bytes > 0:
                        # Jika tahu expected size, cek 95% match
                        is_valid = temp_size >= expected_bytes * 0.95
                    elif temp_size > min_valid_size:
                        # Jika tidak tahu expected size tapi file besar, anggap valid
                        is_valid = True
                    elif temp_size > 100000:  # 100KB minimum
                        # File di atas 100KB anggap valid
                        is_valid = True
                    
                    if is_valid:
                        # Rename dari temp ke final
                        if os.path.exists(filepath):
                            try:
                                os.remove(filepath)
                            except:
                                pass
                        os.rename(temp_filepath, filepath)
                        return True
                    else:
                        # File terlalu kecil atau tidak lengkap
                        if os.path.exists(temp_filepath):
                            os.remove(temp_filepath)
                        # Jika ini bukan attempt terakhir, retry
                        if attempt < 2:
                            time.sleep(2)
                        continue
                        
            except requests.exceptions.Timeout:
                # Timeout, hapus temp dan retry
                if os.path.exists(temp_filepath):
                    try:
                        os.remove(temp_filepath)
                    except:
                        pass
                if attempt < 2:
                    time.sleep(3)  # Wait sebelum retry
                continue
            except requests.exceptions.ConnectionError:
                # Connection error, retry
                if os.path.exists(temp_filepath):
                    try:
                        os.remove(temp_filepath)
                    except:
                        pass
                if attempt < 2:
                    time.sleep(3)
                continue
            except Exception as e:
                # Error lain
                if os.path.exists(temp_filepath):
                    try:
                        os.remove(temp_filepath)
                    except:
                        pass
                if attempt < 2:
                    time.sleep(2)
                    continue
                break
    
    # Cleanup jika masih ada temp file
    if os.path.exists(temp_filepath):
        try:
            os.remove(temp_filepath)
        except:
            pass
    
    return False

# --- LOGIKA CACHE NAMA SEQUENCE ---
PARENT_NAME_CACHE = {}

def get_parent_name_direct(parent_id, headers):
    """Tanya langsung ke server ID ini namanya siapa."""
    if not parent_id: return "No_Parent"
    
    if parent_id in PARENT_NAME_CACHE:
        return PARENT_NAME_CACHE[parent_id]
    
    try:
        url = f"{KITSU_HOST}/data/entities/{parent_id}"
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            name = data.get('name', 'Unknown_Parent')
            PARENT_NAME_CACHE[parent_id] = name 
            return name
    except:
        pass
    
    return f"Parent_{parent_id[:8]}" 

def resolve_sequence_name(entity, seq_map, headers):
    """Deteksi Nama Sequence"""
    if entity.get('sequence_name'):
        return entity['sequence_name']

    seq_id = entity.get('sequence_id')
    if seq_id and seq_id in seq_map:
        return seq_map[seq_id]

    parent_id = entity.get('parent_id')
    if parent_id and parent_id in seq_map:
        return seq_map[parent_id]
        
    if parent_id:
        real_name = get_parent_name_direct(parent_id, headers)
        return real_name

    return "No_Sequence"

def normalize_list_response(payload):
    """Normalisasi response list dari API (kadang dibungkus key data)."""
    if isinstance(payload, dict) and 'data' in payload:
        payload = payload['data']
    return payload if isinstance(payload, list) else []

def get_episodes_for_project(project, headers):
    """Ambil daftar episode; fallback ke request manual jika modul gazu.episode tidak ada."""
    if hasattr(gazu, "episode") and hasattr(gazu.episode, "all_episodes_for_project"):
        try:
            return gazu.episode.all_episodes_for_project(project)
        except Exception:
            pass
    try:
        url = f"{KITSU_HOST}/data/episodes?project_id={project['id']}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            return normalize_list_response(r.json())
    except Exception:
        pass
    return []

def get_sequences_for_episode(episode, headers):
    """Ambil sequence untuk sebuah episode; gunakan REST jika fungsi gazu tidak ada."""
    if hasattr(gazu, "sequence") and hasattr(gazu.sequence, "all_sequences_for_episode"):
        try:
            return gazu.sequence.all_sequences_for_episode(episode)
        except Exception:
            pass
    try:
        url = f"{KITSU_HOST}/data/sequences?episode_id={episode['id']}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            return normalize_list_response(r.json())
    except Exception:
        pass
    return []

def get_sequences_for_project(project, headers):
    """Ambil sequence langsung dari project (untuk sequence tanpa episode)."""
    if hasattr(gazu, "sequence") and hasattr(gazu.sequence, "all_sequences_for_project"):
        try:
            return gazu.sequence.all_sequences_for_project(project)
        except Exception:
            pass
    try:
        url = f"{KITSU_HOST}/data/sequences?project_id={project['id']}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            return normalize_list_response(r.json())
    except Exception:
        pass
    return []

def resolve_episode_and_sequence(entity, seq_map, seq_episode_map, episode_map, headers):
    """Mengembalikan nama Episode dan Sequence untuk sebuah Shot."""
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
            entity,
            seq_map=seq_map,
            seq_episode_map=seq_episode_map,
            episode_map=episode_map,
            headers=headers
        )
        base_folder = os.path.join(root_folder, episode_name, seq_name, entity_name)
    else:
        type_name = sanitize(entity.get('asset_type_name', 'Props'))
        base_folder = os.path.join(root_folder, "Assets", type_name, entity_name)

    # 1. CEK PREVIEW
    if entity.get('preview_file_id'):
        try:
            pf = gazu.files.get_preview_file(entity['preview_file_id'])
            if pf:
                base_name = pf.get('original_name') or pf.get('name') or entity_name
                ext = pf.get('extension', 'mp4')
                clean_name = sanitize(base_name)
                if not clean_name.lower().endswith(f".{ext}"):
                    clean_name = f"{clean_name}.{ext}"

                download_queue.append({
                    'type': 'preview',
                    'id': pf['id'],
                    'url': get_full_url(pf.get('url')),
                    'folder': base_folder, 
                    'filename': clean_name,
                    'size': pf.get('file_size', 0)
                })
        except: pass

    # 2. CEK TASKS
    try:
        if entity_type == 'Shot':
            tasks = gazu.task.all_tasks_for_shot(entity)
        else:
            tasks = gazu.task.all_tasks_for_asset(entity)

        for task in tasks:
            task_type = sanitize(task['task_type_name'])
            task_folder = os.path.join(base_folder, task_type)
            
            # Collect semua file untuk task ini dulu
            task_files = []

            # Preview file
            if task.get('preview_file_id'):
                try:
                    pf = gazu.files.get_preview_file(task['preview_file_id'])
                    if pf:
                        base_name = pf.get('original_name') or pf.get('name')
                        ext = pf.get('extension', 'mp4')
                        clean_name = f"{task_type}_Preview_{sanitize(base_name)}.{ext}"

                        task_files.append({
                            'type': 'preview',
                            'id': pf['id'],
                            'url': get_full_url(pf.get('url')),
                            'folder': task_folder,
                            'filename': clean_name,
                            'size': pf.get('file_size', 0)
                        })
                except: pass

            # Output files
            outputs = gazu.files.all_output_files_for_entity(task)
            for out in outputs:
                base_name = out.get('original_name') or out.get('name')
                ext = out.get('extension', '')
                clean_name = f"{task_type}_Output_{sanitize(base_name)}"
                if ext and not clean_name.lower().endswith(f".{ext}"):
                    clean_name = f"{clean_name}.{ext}"
                task_files.append({
                    'type': 'output',
                    'id': out['id'],
                    'url': get_full_url(out.get('url')),
                    'folder': task_folder,
                    'filename': clean_name,
                    'size': out.get('file_size', 0)
                })

            # Working files
            works = gazu.files.all_working_files_for_entity(task)
            for work in works:
                base_name = work.get('original_name') or work.get('name')
                ext = work.get('extension', '')
                clean_name = f"{task_type}_SRC_{sanitize(base_name)}"
                if ext and not clean_name.lower().endswith(f".{ext}"):
                    clean_name = f"{clean_name}.{ext}"
                task_files.append({
                    'type': 'working',
                    'id': work['id'],
                    'url': get_full_url(work.get('url')),
                    'folder': task_folder,
                    'filename': clean_name,
                    'size': work.get('file_size', 0)
                })

            # Hanya tambahkan ke download_queue jika ada file
            if task_files:
                download_queue.extend(task_files)

    except Exception: pass

def main():
    global KITSU_HOST  # Mengakses variable global
    print("="*60)
    print("   KITSU DOWNLOADER (Universal)")
    print("="*60)

    # --- 1. SETUP KONEKSI ---
    while True:
        try:
            # Input URL Server
            if not KITSU_HOST:
                host_input = input("Masukkan URL Server Kitsu (cth: http://192.168.1.50/api): ").strip()
                if not host_input:
                    print("URL tidak boleh kosong.")
                    continue
                # Pastikan ada /api di ujungnya jika user lupa
                if not host_input.endswith("/api"):
                    host_input = f"{host_input}/api"
                KITSU_HOST = host_input

            print(f"Target Server: {KITSU_HOST}")
            user_input = input("Username/Email : ")
            pass_input = getpass.getpass("Password       : ")
            
            gazu.client.set_host(KITSU_HOST)
            gazu.log_in(user_input, pass_input)
            print(">> Login BERHASIL!\n")
            break
        except Exception as e:
            print(f"!! Login GAGAL: {e}")
            print("Cek kembali URL Server dan Username/Password Anda.\n")
            KITSU_HOST = "" # Reset URL jika gagal agar bisa input ulang

    # --- 2. AMBIL TOKEN ---
    auth_headers = None
    try:
        client_instance = gazu.client.default_client
        raw_tokens = client_instance.tokens
        auth_headers = {"Authorization": f"Bearer {raw_tokens['access_token']}"}
    except Exception as e:
        print(f"[X] Gagal ambil token: {e}")
        return

    # --- 3. PILIH PROJECT ---
    try:
        all_projects = gazu.project.all_open_projects()
    except:
        print("Gagal ambil project."); return

    if not all_projects:
        print("Tidak ada project aktif ditemukan."); return

    print("\n--- DAFTAR PROJECT ---")
    for idx, proj in enumerate(all_projects):
        print(f"[{idx+1}] {proj['name']}")
    print("----------------------")

    selected_project = None
    while True:
        try:
            choice = input("Pilih nomor project: ")
            idx = int(choice) - 1
            if 0 <= idx < len(all_projects):
                selected_project = all_projects[idx]
                break
        except ValueError: pass

    # --- 4. PERSIAPAN FOLDER ---
    home_dir = os.path.expanduser("~")
    downloads_path = os.path.join(home_dir, "Downloads")
    folder_name = f"Kitsu_{sanitize(selected_project['name'])}"
    download_root = os.path.join(downloads_path, folder_name)
    
    # Buat folder root jika belum ada
    try:
        os.makedirs(download_root, exist_ok=True)
    except Exception as e:
        print(f"[X] Gagal membuat folder: {e}")
        return

    # --- 5. DEEP MAPPING ---
    print("\n>> Membangun Peta Struktur (Episodes & Sequences)...")
    seq_map = {}
    episode_map = {}
    seq_episode_map = {}
    
    try:
        episodes = get_episodes_for_project(selected_project, auth_headers)
        print(f"   Ditemukan {len(episodes)} Episodes.")
        for ep in episodes:
            episode_map[ep['id']] = ep['name']
            seqs = get_sequences_for_episode(ep, auth_headers)
            for s in seqs:
                seq_map[s['id']] = s['name']
                seq_episode_map[s['id']] = ep['id']
        
        root_seqs = get_sequences_for_project(selected_project, auth_headers)
        for s in root_seqs:
            seq_map[s['id']] = s['name']
            ep_id = s.get('episode_id')
            if ep_id:
                seq_episode_map[s['id']] = ep_id
                if ep_id not in episode_map:
                    episode_map[ep_id] = get_parent_name_direct(ep_id, auth_headers)
        print(f"   Total Sequence terdata: {len(seq_map)}")
    except Exception as e:
        print(f"   Warning: Mapping struktur tidak sempurna ({e})")

    # --- 6. SCANNING ---
    print(f"\n>> Menganalisis File di '{selected_project['name']}'...")
    
    download_queue = []
    
    shots = gazu.shot.all_shots_for_project(selected_project)
    print(f"   Memproses {len(shots)} Shots...")
    
    for i, shot in enumerate(shots):
        sys.stdout.write(f"\r   Scanning {i+1}/{len(shots)}...")
        sys.stdout.flush()
        scan_entity(
            shot,
            download_root,
            'Shot',
            download_queue,
            seq_map,
            episode_map,
            seq_episode_map,
            headers=auth_headers
        )

    assets = gazu.asset.all_assets_for_project(selected_project)
    if assets:
        print(f"\n   Memproses {len(assets)} Assets...")
        for i, asset in enumerate(assets):
            sys.stdout.write(f"\r   Scanning {i+1}/{len(assets)}...")
            sys.stdout.flush()
            scan_entity(
                asset,
                download_root,
                'Asset',
                download_queue,
                seq_map,
                episode_map,
                seq_episode_map,
                headers=auth_headers
            )

    # --- 7. EKSEKUSI ---
    total_files = len(download_queue)
    total_size = sum(item.get('size', 0) for item in download_queue)
    human_size = format_bytes(total_size)

    print("\n\n" + "="*60)
    print("   HASIL ANALISIS")
    print("="*60)
    
    if total_files == 0:
        print("!! TIDAK ADA FILE !!")
        return

    print(f"Total File      : {total_files} files")
    print(f"Estimasi Ukuran : {human_size}")
    print(f"LOKASI SIMPAN   : {download_root}")
    print("-" * 60)

    while True:
        confirm = input(f">> Download {human_size}? (y/n): ").lower()
        if confirm == 'y': break
        if confirm == 'n': print("Batal."); return

    print("\n>> Memulai Download...")
    print(f"   Menyimpan ke: {download_root}")
    print("   (File < 1MB akan di-redownload otomatis jika sudah ada)\n")
    success_count = 0
    failed_count = 0
    downloaded_size = 0
    start_time = time.time()

    for i, item in enumerate(download_queue):
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
    actual_downloaded = format_bytes(downloaded_size)
    
    # Hitung total file yang ada di folder
    total_downloaded_files = 0
    total_downloaded_bytes = 0
    try:
        for root, dirs, files in os.walk(download_root):
            for file in files:
                if not file.endswith('.tmp'):
                    total_downloaded_files += 1
                    total_downloaded_bytes += os.path.getsize(os.path.join(root, file))
    except:
        pass
    
    actual_size_display = format_bytes(total_downloaded_bytes)
    
    print(f"\n\n" + "="*60)
    print(f"SELESAI dalam {duration:.1f} detik.")
    print(f"Sukses   : {success_count} / {total_files} file")
    print(f"Gagal    : {failed_count} / {total_files} file")
    print(f"File Ada : {total_downloaded_files} file ({actual_size_display})")
    print(f"Estimasi : {human_size}")
    print(f"Folder   : {download_root}")
    print("="*60)

if __name__ == "__main__":
    main()

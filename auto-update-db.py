#!/usr/bin/env python
# coding: utf-8

# In[14]:


"""
Automated daily update script for Leumit League
Fully self-contained - can run independently or via FastAPI
Runs without user intervention:
1. Updates player details (basic info + history)
2. Scrapes new game statistics
3. Calculates averages

Schedule this script to run daily via cron/Task Scheduler
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from pathlib import Path
from datetime import datetime

BASE_URL = "https://ibasketball.co.il"
LEUMIT_URL = "https://ibasketball.co.il/league/2025-2/"
LEAGUE_NAME = "leumit"
DATA_FOLDER = "data/leumit"
GAMES_FOLDER = "data/leumit/leumit_games"
LOG_FILE = "data/leumit/update_log.txt"
TEAMS_CSV = "data/leumit/team_names.csv"  # Updated path

# ============================================
# TEAM NAME MAPPING
# ============================================

def load_team_mapping():
    """Load team name mapping from CSV"""
    try:
        if os.path.exists(TEAMS_CSV):
            df = pd.read_csv(TEAMS_CSV, encoding='utf-8-sig')
            # Create mapping dictionary: map ALL name variations to normalized_name
            mapping = {}
            for _, row in df.iterrows():
                normalized = row['normalized_name']
                # Map all variations to normalized name
                mapping[row['player_details_name']] = normalized
                mapping[row['schedule_team_name']] = normalized
                mapping[normalized] = normalized  # Map to itself for consistency
                
                # Also map short name if different
                if 'short_name' in row and pd.notna(row['short_name']):
                    mapping[row['short_name']] = normalized
                    
            log_message(f"✅ Loaded team mapping: {len(df)} teams, {len(mapping)} name variations")
            return mapping
        else:
            log_message(f"⚠️  Team mapping file not found: {TEAMS_CSV}")
            log_message("   Continuing without team name normalization")
            return {}
    except Exception as e:
        log_message(f"⚠️  Error loading team mapping: {e}")
        return {}

def normalize_team_name(team_name, team_mapping):
    """Normalize team name using mapping dictionary - always returns normalized_name"""
    if not team_mapping:
        return team_name
    
    # Try exact match first
    if team_name in team_mapping:
        return team_mapping[team_name]
    
    # Try with stripped whitespace
    team_name_stripped = team_name.strip()
    if team_name_stripped in team_mapping:
        return team_mapping[team_name_stripped]
    
    # If no match found, return original and log warning
    log_message(f"   ⚠️  No mapping found for team: '{team_name}'")
    return team_name

# ============================================
# LOGGING
# ============================================

def log_message(message):
    """Log message to both console and file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    
    # Ensure log directory exists
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + "\n")

# ============================================
# SHARED UTILITIES
# ============================================

def get_soup(url):
    """Fetch URL and return BeautifulSoup object"""
    try:
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        log_message(f"❌ Error fetching {url}: {e}")
        return None

def normalize_season(season_str):
    """Convert '2024-2025' to '2024-25' format"""
    parts = season_str.split('-')
    if len(parts) == 2:
        return f"{parts[0]}-{parts[1][-2:]}"
    return season_str

def save_to_csv(data, filepath, columns=None):
    """Save data to CSV with UTF-8-BOM encoding"""
    df = pd.DataFrame(data)
    df = df.dropna(axis=1, how='all')
    
    if columns:
        existing_cols = [col for col in columns if col in df.columns]
        extra_cols = [col for col in df.columns if col not in columns]
        df = df[existing_cols + extra_cols]
    
    df.to_csv(filepath, index=False, encoding='utf-8-sig')

def append_to_csv(new_data, filepath, columns=None):
    """Append new data to existing CSV or create new one"""
    df_new = pd.DataFrame(new_data)
    df_new = df_new.dropna(axis=1, how='all')
    
    if os.path.exists(filepath):
        try:
            df_existing = pd.read_csv(filepath, encoding='utf-8-sig')
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        except Exception as e:
            log_message(f"⚠️  Could not read existing file, creating new: {e}")
            df_combined = df_new
    else:
        df_combined = df_new
    
    if columns:
        existing_cols = [col for col in columns if col in df_combined.columns]
        other_cols = [col for col in df_combined.columns if col not in columns]
        df_combined = df_combined[existing_cols + other_cols]
    
    df_combined.to_csv(filepath, index=False, encoding='utf-8-sig')

# ============================================
# PLAYER DETAILS SCRAPING
# ============================================

def scrape_player_list(league_url):
    soup = get_soup(league_url)
    if not soup:
        return []
    
    players = []
    for player_tag in soup.select(".player-gallery a.player"):
        name = player_tag.get_text("|", strip=True).split('|')[0]
        team = player_tag.find("span").get_text(strip=True)
        player_url = player_tag["href"]
        players.append({"Name": name, "Team": team, "URL": player_url})
    
    return players

def scrape_player_details(player_url):
    soup = get_soup(player_url)
    if not soup:
        return {"Date Of Birth": "", "Height": "", "Number": ""}
    
    dob = soup.find("div", class_="data-birthdate")
    height = soup.find("div", class_="data-other", attrs={"data-metric": "גובה"})
    
    # Find player number
    number = ""
    general_ul = soup.find("ul", class_="general")
    if general_ul:
        for li in general_ul.find_all("li"):
            label = li.find("span", class_="label")
            if label and "מספר" in label.text:
                data_span = li.find("span", class_="data-number")
                if data_span:
                    number = data_span.get_text(strip=True)
                    break
    
    dob_text = dob.get_text("|", strip=True).split("|")[-1] if dob else ""
    dob_formatted = "/".join(dob_text.split("-")[::-1]) if dob_text else ""
    
    return {
        "Date Of Birth": dob_formatted,
        "Height": height.get_text("|", strip=True).split("|")[-1] if height else "",
        "Number": number
    }

def scrape_player_history(player_url):
    soup = get_soup(player_url)
    if not soup:
        return {}
    
    history_tag = soup.find("div", class_="data-teams")
    history = {}
    youth_count = 0
    
    if history_tag:
        br_tags = history_tag.find_all('br')
        
        for br in br_tags:
            season_span = br.find_next_sibling('span', title=True)
            
            if season_span:
                season_raw = season_span.get_text(strip=True)
                season = normalize_season(season_raw)
                
                team_link = season_span.find_next_sibling('a')
                if team_link:
                    team = team_link.get_text(strip=True)
                    league_link = team_link.find_next_sibling('a')
                    
                    if league_link:
                        league = league_link.get_text(strip=True)
                        
                        if "נוער" in league:
                            youth_count += 1
                            if youth_count > 1:
                                break
                        
                        if season in history:
                            history[season] += f", {team} ({league})"
                        else:
                            history[season] = f"{team} ({league})"
    
    return history

def load_existing_data(folder, details_file, history_file):
    """Load existing CSV data if available"""
    existing_details = {}
    existing_history = {}
    
    details_path = os.path.join(folder, details_file)
    history_path = os.path.join(folder, history_file)
    
    if os.path.exists(details_path):
        df_details = pd.read_csv(details_path, encoding='utf-8-sig')
        for _, row in df_details.iterrows():
            existing_details[row['Name']] = row.to_dict()
    
    if os.path.exists(history_path):
        df_history = pd.read_csv(history_path, encoding='utf-8-sig')
        for _, row in df_history.iterrows():
            existing_history[row['Name']] = row.to_dict()
    
    return existing_details, existing_history

def has_any_history(player_name, existing_history):
    """Check if player has ANY history data at all"""
    if player_name not in existing_history:
        return False
    
    player_history = existing_history[player_name]
    for key, value in player_history.items():
        if key not in ["Name", "Current Team", "Date Of Birth", "Height", "Number"]:
            if not pd.isna(value) and str(value).strip() != "":
                return True
    return False

def needs_scraping(player_name, existing_details, existing_history):
    """Check if we need to scrape this player's page"""
    if player_name not in existing_details:
        return True, "New player"
    
    player_details = existing_details[player_name]
    dob = player_details.get("Date Of Birth", "")
    height = player_details.get("Height", "")
    number = player_details.get("Number", "")
    
    if pd.isna(dob) or str(dob).strip() == "":
        return True, "Missing DOB"
    if pd.isna(height) or str(height).strip() == "":
        return True, "Missing Height"
    if pd.isna(number) or str(number).strip() == "":
        return True, "Missing Number"
    
    if not has_any_history(player_name, existing_history):
        return True, "No history data"
    
    return False, "Complete data"

def update_player_details():
    """Update player details for Leumit league"""
    log_message("="*60)
    log_message("STEP 1: UPDATING PLAYER DETAILS")
    log_message("="*60)
    
    # Load team mapping
    team_mapping = load_team_mapping()
    
    Path(DATA_FOLDER).mkdir(parents=True, exist_ok=True)
    
    details_file = f"{LEAGUE_NAME}_player_details.csv"
    history_file = f"{LEAGUE_NAME}_player_history.csv"
    
    existing_details, existing_history = load_existing_data(DATA_FOLDER, details_file, history_file)
    
    log_message("Fetching player list...")
    players = scrape_player_list(LEUMIT_URL)
    
    if not players:
        log_message("❌ ERROR: No players found")
        return False
    
    log_message(f"Found {len(players)} players")
    
    details_data = []
    history_data = []
    all_seasons = set()
    
    new_players = 0
    updated_players = 0
    skipped_players = 0
    
    for i, player in enumerate(players, 1):
        player_name = player['Name']
        current_team_raw = player['Team']
        
        # Normalize team name
        current_team = normalize_team_name(current_team_raw, team_mapping)
        if current_team != current_team_raw:
            log_message(f"   Normalized: '{current_team_raw}' → '{current_team}'")
        
        should_scrape, reason = needs_scraping(player_name, existing_details, existing_history)
        
        if should_scrape:
            log_message(f"[{i}/{len(players)}] Scraping: {player_name} ({reason})")
            
            details = scrape_player_details(player["URL"])
            history = scrape_player_history(player["URL"])
            
            all_seasons.update(history.keys())
            
            if player_name not in existing_details:
                new_players += 1
            else:
                updated_players += 1
            
            time.sleep(1)
        else:
            details = {
                "Date Of Birth": existing_details[player_name].get("Date Of Birth", ""),
                "Height": existing_details[player_name].get("Height", ""),
                "Number": existing_details[player_name].get("Number", "")
            }
            
            if player_name in existing_history:
                history = {k: v for k, v in existing_history[player_name].items() 
                          if k not in ["Name", "Current Team", "Date Of Birth", "Height", "Number"]}
                all_seasons.update(history.keys())
            else:
                history = {}
            
            skipped_players += 1
        
        new_details = {
            "Name": player_name,
            "Team": current_team,  # Use normalized name
            "Date Of Birth": details["Date Of Birth"],
            "Height": details["Height"],
            "Number": details["Number"]
        }
        
        new_history = {
            "Name": player_name,
            "Current Team": current_team,  # Use normalized name
            "Date Of Birth": details["Date Of Birth"],
            "Height": details["Height"],
            "Number": details["Number"]
        }
        new_history.update(history)
        
        details_data.append(new_details)
        history_data.append(new_history)
    
    sorted_seasons = sorted(list(all_seasons), reverse=True)
    
    details_path = os.path.join(DATA_FOLDER, details_file)
    history_path = os.path.join(DATA_FOLDER, history_file)
    
    save_to_csv(details_data, details_path, ["Name", "Team", "Date Of Birth", "Height", "Number"])
    save_to_csv(history_data, history_path, ["Name", "Current Team", "Date Of Birth", "Height", "Number"] + sorted_seasons)
    
    log_message(f"✅ Player details updated")
    log_message(f"   File: {details_path}")
    log_message(f"   File: {history_path}")
    log_message(f"   Total: {len(players)} | New: {new_players} | Updated: {updated_players} | Skipped: {skipped_players}")
    
    return True

# ============================================
# GAME DETAILS SCRAPING
# ============================================

def extract_league_id(league_url):
    """Extract league_id from the Excel export link"""
    soup = get_soup(league_url)
    if not soup:
        return None
    
    export_link = soup.find('a', class_='export')
    if export_link and 'href' in export_link.attrs:
        href = export_link['href']
        if 'league_id=' in href:
            league_id = href.split('league_id=')[1].split('&')[0]
            return league_id
    
    return None

def download_games_excel(league_url, output_folder):
    """Download games Excel file and save as CSV"""
    league_id = extract_league_id(league_url)
    if not league_id:
        log_message("❌ Could not find league_id")
        return None
    
    base_url = league_url.rstrip('/')
    excel_url = f"{base_url}/?feed=xlsx&league_id={league_id}"
    
    try:
        response = requests.get(excel_url, timeout=15)
        response.raise_for_status()
        
        temp_excel = os.path.join(output_folder, 'temp_games.xlsx')
        with open(temp_excel, 'wb') as f:
            f.write(response.content)
        
        df = pd.read_excel(temp_excel)
        
        csv_path = os.path.join(output_folder, 'games_schedule.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        os.remove(temp_excel)
        
        log_message(f"✅ Games schedule updated: {len(df)} games")
        
        return df
        
    except Exception as e:
        log_message(f"❌ Error downloading games: {e}")
        return None

def scrape_quarter_scores(soup, game_id, team_mapping):
    """Scrape quarter-by-quarter scores"""
    quarters_data = []
    
    try:
        results_table = soup.find('table', class_='sp-event-results')
        if not results_table:
            return quarters_data
        
        rows = results_table.find('tbody').find_all('tr')
        
        teams = []
        for row in rows:
            team_cell = row.find('td', class_='data-name')
            if team_cell:
                team_link = team_cell.find('a')
                team_name_raw = team_link.text.strip() if team_link else team_cell.text.strip()
                # Normalize team name
                team_name = normalize_team_name(team_name_raw, team_mapping)
                teams.append(team_name)
        
        if len(teams) != 2:
            return quarters_data
        
        for idx, row in enumerate(rows):
            team_name = teams[idx]
            opponent_name = teams[1 - idx]
            
            q1 = row.find('td', class_='data-one')
            q2 = row.find('td', class_='data-two')
            q3 = row.find('td', class_='data-three')
            q4 = row.find('td', class_='data-four')
            
            quarters = [
                ('Q1', q1.text.strip() if q1 else '0'),
                ('Q2', q2.text.strip() if q2 else '0'),
                ('Q3', q3.text.strip() if q3 else '0'),
                ('Q4', q4.text.strip() if q4 else '0')
            ]
            
            opponent_row = rows[1 - idx]
            opp_q1 = opponent_row.find('td', class_='data-one')
            opp_q2 = opponent_row.find('td', class_='data-two')
            opp_q3 = opponent_row.find('td', class_='data-three')
            opp_q4 = opponent_row.find('td', class_='data-four')
            
            opponent_quarters = [
                opp_q1.text.strip() if opp_q1 else '0',
                opp_q2.text.strip() if opp_q2 else '0',
                opp_q3.text.strip() if opp_q3 else '0',
                opp_q4.text.strip() if opp_q4 else '0'
            ]
            
            for (quarter, score), opp_score in zip(quarters, opponent_quarters):
                quarters_data.append({
                    'game_id': game_id,
                    'team': team_name,
                    'opponent': opponent_name,
                    'quarter': quarter,
                    'score': int(score) if score.isdigit() else 0,
                    'score_against': int(opp_score) if opp_score.isdigit() else 0
                })
        
        return quarters_data
        
    except Exception as e:
        log_message(f"   ❌ Error parsing quarters: {e}")
        return quarters_data

def split_shooting_stats(player_data):
    """Split shooting stats into separate columns"""
    
    if 'fgs' in player_data:
        if '-' in str(player_data['fgs']):
            parts = player_data['fgs'].split('-')
            player_data['2ptm'] = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
            player_data['2pta'] = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
            del player_data['fgs']
    
    if 'threeps' in player_data:
        if '-' in str(player_data['threeps']):
            parts = player_data['threeps'].split('-')
            player_data['3ptm'] = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
            player_data['3pta'] = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
            del player_data['threeps']
    
    if 'fts' in player_data:
        if '-' in str(player_data['fts']):
            parts = player_data['fts'].split('-')
            player_data['ftm'] = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
            player_data['fta'] = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
            del player_data['fts']
    
    for key in ['2ptm', '2pta', '3ptm', '3pta', 'ftm', 'fta']:
        if key in player_data:
            if isinstance(player_data[key], str):
                val = player_data[key].strip()
                player_data[key] = int(val) if val.isdigit() else 0
            elif not isinstance(player_data[key], int):
                try:
                    player_data[key] = int(player_data[key])
                except:
                    player_data[key] = 0
    
    two_ptm = player_data.get('2ptm', 0)
    two_pta = player_data.get('2pta', 0)
    three_ptm = player_data.get('3ptm', 0)
    three_pta = player_data.get('3pta', 0)
    
    player_data['fgm'] = two_ptm + three_ptm
    player_data['fga'] = two_pta + three_pta
    
    if two_pta > 0:
        player_data['2pt_pct'] = round((two_ptm / two_pta) * 100, 1)
    else:
        player_data['2pt_pct'] = 0.0
    
    if three_pta > 0:
        player_data['3pt_pct'] = round((three_ptm / three_pta) * 100, 1)
    else:
        player_data['3pt_pct'] = 0.0
    
    if player_data['fga'] > 0:
        player_data['fg_pct'] = round((player_data['fgm'] / player_data['fga']) * 100, 1)
    else:
        player_data['fg_pct'] = 0.0
    
    ftm = player_data.get('ftm', 0)
    fta = player_data.get('fta', 0)
    if fta > 0:
        player_data['ft_pct'] = round((ftm / fta) * 100, 1)
    else:
        player_data['ft_pct'] = 0.0
    
    for key in ['fgpercent', 'threeppercent', 'ftpercent']:
        if key in player_data:
            del player_data[key]
    
    return player_data

def scrape_player_stats(soup, game_id, team_mapping):
    """Scrape player statistics"""
    player_stats = []
    
    try:
        performance_sections = soup.find_all('div', class_='sp-template-event-performance-values')
        
        for section in performance_sections:
            team_header = section.find('h4', class_='sp-table-caption')
            if not team_header:
                continue
            
            team_name_raw = team_header.text.strip()
            # Normalize team name
            team_name = normalize_team_name(team_name_raw, team_mapping)
            
            table = section.find('table', class_='sp-event-performance')
            if not table:
                continue
            
            headers = []
            thead = table.find('thead')
            if thead:
                header_row = thead.find('tr')
                for th in header_row.find_all('th'):
                    headers.append(th.text.strip())
            
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            for row in tbody.find_all('tr'):
                if 'sp-total-row' in row.get('class', []):
                    continue
                
                player_data = {
                    'game_id': game_id,
                    'team': team_name  # Use normalized name
                }
                
                row_classes = row.get('class', [])
                player_data['starter'] = 1 if 'lineup' in row_classes else 0
                
                cells = row.find_all('td')
                
                for idx, cell in enumerate(cells):
                    if idx < len(headers):
                        header = headers[idx]
                        
                        if header == 'שחקן' or 'data-name' in cell.get('class', []):
                            player_link = cell.find('a')
                            if player_link:
                                player_data['player_name'] = player_link.text.strip()
                                player_data['player_url'] = player_link['href']
                            else:
                                player_data['player_name'] = cell.text.strip()
                        else:
                            data_key = cell.get('data-key', header)
                            player_data[data_key] = cell.text.strip()
                
                if 'player_name' in player_data and player_data['player_name']:
                    minutes = player_data.get('min', '00:00')
                    if minutes != '00:00' and minutes != '0:00':
                        if '#' in player_data:
                            player_data['number'] = player_data.pop('#')
                        
                        if 'min' in player_data:
                            min_str = player_data['min']
                            try:
                                if ':' in min_str:
                                    parts = min_str.split(':')
                                    mins = int(parts[0])
                                    secs = int(parts[1])
                                    if secs >= 30:
                                        mins += 1
                                    player_data['min'] = mins
                                else:
                                    player_data['min'] = int(min_str)
                            except:
                                player_data['min'] = 0
                        
                        player_data.pop('pm', None)
                        player_data = split_shooting_stats(player_data)
                        player_stats.append(player_data)
        
        return player_stats
        
    except Exception as e:
        log_message(f"   ❌ Error parsing player stats: {e}")
        return player_stats

def scrape_team_stats(soup, game_id):
    """Scrape team statistics"""
    team_stats = []
    
    try:
        performance_sections = soup.find_all('div', class_='sp-template-event-performance-values')
        
        for section in performance_sections:
            team_header = section.find('h4', class_='sp-table-caption')
            if not team_header:
                continue
            
            team_name = team_header.text.strip()
            
            table = section.find('table', class_='sp-event-performance')
            if not table:
                continue
            
            thead = table.find('thead')
            header_keys = []
            if thead:
                header_row = thead.find('tr')
                for th in header_row.find_all('th'):
                    data_key = None
                    th_classes = th.get('class', [])
                    for cls in th_classes:
                        if cls.startswith('data-'):
                            data_key = cls.replace('data-', '')
                            break
                    header_keys.append(data_key)
            
            total_row = None
            tfoot = table.find('tfoot')
            if tfoot:
                total_row = tfoot.find('tr', class_='sp-total-row')
            
            if not total_row:
                tbody = table.find('tbody')
                if tbody:
                    all_rows = tbody.find_all('tr')
                    for row in reversed(all_rows):
                        name_cell = row.find('td', class_='data-name')
                        if name_cell and 'סך הכל' in name_cell.text:
                            total_row = row
                            break
            
            if total_row:
                stats_dict = {
                    'game_id': game_id,
                    'team': team_name
                }
                
                cells = total_row.find_all('td')
                
                for idx, cell in enumerate(cells):
                    cell_classes = cell.get('class', [])
                    if 'data-name' in cell_classes:
                        continue
                    
                    data_key = None
                    for cls in cell_classes:
                        if cls.startswith('data-'):
                            data_key = cls.replace('data-', '')
                            break
                    
                    if not data_key and idx < len(header_keys):
                        data_key = header_keys[idx]
                    
                    if data_key:
                        value = cell.text.strip()
                        stats_dict[data_key] = value
                
                stats_dict = split_shooting_stats(stats_dict)
                
                stats_dict.pop('min', None)
                stats_dict.pop('pm', None)
                stats_dict.pop('#', None)
                stats_dict.pop('number', None)
                
                team_stats_div = section.find('div', class_='team-stats')
                
                if team_stats_div:
                    labels = team_stats_div.find_all('label')
                    for label in labels:
                        stat_text = label.contents[0].strip() if label.contents else ''
                        stat_value_span = label.find('span')
                        
                        if stat_value_span:
                            stat_value = stat_value_span.text.strip()
                            
                            stat_mapping = {
                                'נקודות מהזדמנות שנייה:': 'second_chance_pts',
                                'נקודות ספסל:': 'bench_pts',
                                'נקודות ממתפרצת:': 'fast_break_pts',
                                'נקודות בצבע:': 'points_in_paint',
                                'נקודות מאיבודים:': 'pts_off_turnovers'
                            }
                            
                            stat_key = stat_mapping.get(stat_text, stat_text)
                            stats_dict[stat_key] = int(stat_value) if stat_value.isdigit() else stat_value
                
                team_stats.append(stats_dict)
        
        return team_stats
        
    except Exception as e:
        log_message(f"   ❌ Error parsing team stats: {e}")
        return team_stats

def scrape_game_details(game_id, team_mapping):
    """Scrape all details for a single game"""
    game_url = f"https://ibasketball.co.il/match/{game_id}/"
    
    soup = get_soup(game_url)
    if not soup:
        return None, None, None
    
    quarters = scrape_quarter_scores(soup, game_id, team_mapping)
    players = scrape_player_stats(soup, game_id, team_mapping)
    teams = scrape_team_stats(soup, game_id, team_mapping)
    
    return quarters, players, teams

def load_existing_game_ids(output_folder):
    """Load game IDs that have already been scraped"""
    existing_ids = set()
    
    quarters_path = os.path.join(output_folder, 'game_quarters.csv')
    if os.path.exists(quarters_path):
        try:
            df = pd.read_csv(quarters_path, encoding='utf-8-sig')
            if 'game_id' in df.columns:
                existing_ids.update(df['game_id'].astype(str).unique())
        except Exception as e:
            log_message(f"   ⚠️  Could not read existing quarters data: {e}")
    
    return existing_ids

def scrape_all_games(games_df, output_folder, team_mapping):
    """Scrape details for all completed games"""
    
    if 'Home Score' not in games_df.columns:
        log_message("❌ 'Home Score' column not found")
        return False
    
    completed_games = games_df[games_df['Home Score'].notna() & (games_df['Home Score'] != '')]
    log_message(f"   Found {len(completed_games)} completed games")
    
    existing_game_ids = load_existing_game_ids(output_folder)
    if existing_game_ids:
        log_message(f"   Already scraped: {len(existing_game_ids)} games")
    
    games_to_scrape = []
    for idx, row in completed_games.iterrows():
        game_code = row.get('Code')
        if pd.isna(game_code):
            continue
        
        game_id = str(int(game_code)) if isinstance(game_code, float) else str(game_code)
        
        if game_id not in existing_game_ids:
            games_to_scrape.append((idx, row, game_id))
    
    if not games_to_scrape:
        log_message("   ✅ All games already scraped")
        return True
    
    log_message(f"   Scraping {len(games_to_scrape)} new games")
    
    all_quarters = []
    all_player_stats = []
    all_team_stats = []
    
    for count, (idx, row, game_id) in enumerate(games_to_scrape, 1):
        home_team = row.get('Home Team', '')
        away_team = row.get('Away Team', '')
        
        log_message(f"   [{count}/{len(games_to_scrape)}] Game {game_id}: {home_team} vs {away_team}")
        
        try:
            quarters, players, teams = scrape_game_details(game_id, team_mapping)
            
            if quarters:
                all_quarters.extend(quarters)
            if players:
                all_player_stats.extend(players)
            if teams:
                all_team_stats.extend(teams)
            
            # If no data at all, log warning
            if not quarters and not players and not teams:
                log_message(f"   ⚠️  No stats found for game {game_id} - may not have detailed stats yet")
            
        except Exception as e:
            log_message(f"   ❌ Error scraping game {game_id}: {e}")
            log_message(f"   Skipping game and continuing...")
            continue
        
        time.sleep(1)
    
    if all_quarters:
        quarters_path = os.path.join(output_folder, 'game_quarters.csv')
        append_to_csv(all_quarters, quarters_path, 
                     columns=['game_id', 'team', 'opponent', 'quarter', 'score', 'score_against'])
        log_message(f"   ✅ Saved: {quarters_path}")
    
    if all_player_stats:
        stats_path = os.path.join(output_folder, 'game_player_stats.csv')
        player_columns = ['game_id', 'team', 'number', 'player_name', 'player_url', 'starter', 'min', 'pts',
                         '2ptm', '2pta', '2pt_pct',
                         '3ptm', '3pta', '3pt_pct',
                         'fgm', 'fga', 'fg_pct',
                         'ftm', 'fta', 'ft_pct',
                         'def', 'off', 'reb', 'pf', 'pfa',
                         'stl', 'to', 'ast', 'blk', 'blka', 'rate', 'pm']
        append_to_csv(all_player_stats, stats_path, columns=player_columns)
        log_message(f"   ✅ Saved: {stats_path}")
    
    if all_team_stats:
        team_stats_path = os.path.join(output_folder, 'game_team_stats.csv')
        team_columns = ['game_id', 'team', 'pts', 
                       '2ptm', '2pta', '2pt_pct',
                       '3ptm', '3pta', '3pt_pct',
                       'fgm', 'fga', 'fg_pct',
                       'ftm', 'fta', 'ft_pct',
                       'def', 'off', 'reb', 'pf', 'pfa', 
                       'stl', 'to', 'ast', 'blk', 'blka', 'rate',
                       'second_chance_pts', 'bench_pts', 'fast_break_pts', 
                       'points_in_paint', 'pts_off_turnovers']
        append_to_csv(all_team_stats, team_stats_path, columns=team_columns)
        log_message(f"   ✅ Saved: {team_stats_path}")
    
    log_message(f"✅ Game stats updated: {len(games_to_scrape)} new games scraped")
    return True

def update_game_details():
    """Update game details for Leumit league"""
    log_message("="*60)
    log_message("STEP 2: UPDATING GAME DETAILS")
    log_message("="*60)
    
    # Load team mapping
    team_mapping = load_team_mapping()
    
    Path(GAMES_FOLDER).mkdir(parents=True, exist_ok=True)
    
    games_df = download_games_excel(LEUMIT_URL, GAMES_FOLDER)
    
    if games_df is None or len(games_df) == 0:
        log_message("❌ No games found")
        return False
    
    return scrape_all_games(games_df, GAMES_FOLDER, team_mapping)

# ============================================
# CALCULATE AVERAGES
# ============================================

def calculate_averages():
    """Calculate player, team, and opponent averages"""
    log_message("="*60)
    log_message("STEP 3: CALCULATING AVERAGES")
    log_message("="*60)
    
    player_stats_file = f"{GAMES_FOLDER}/game_player_stats.csv"
    team_stats_file = f"{GAMES_FOLDER}/game_team_stats.csv"
    
    player_averages_file = f"{DATA_FOLDER}/{LEAGUE_NAME}_player_averages.csv"
    team_averages_file = f"{DATA_FOLDER}/{LEAGUE_NAME}_team_averages.csv"
    opponent_averages_file = f"{DATA_FOLDER}/{LEAGUE_NAME}_opponent_averages.csv"
    
    if not os.path.exists(player_stats_file):
        log_message(f"❌ No player stats found")
        return False
    
    if not os.path.exists(team_stats_file):
        log_message(f"❌ No team stats found")
        return False
    
    try:
        player_df = pd.read_csv(player_stats_file, encoding='utf-8-sig')
        team_df = pd.read_csv(team_stats_file, encoding='utf-8-sig')
    except Exception as e:
        log_message(f"❌ Error reading stats files: {e}")
        return False
    
    # PLAYER AVERAGES
    numeric_cols = [
        'pts', '2ptm', '2pta', '3ptm', '3pta', 'fgm', 'fga', 
        'ftm', 'fta', 'def', 'off', 'reb', 'pf', 'pfa', 
        'stl', 'to', 'ast', 'blk', 'blka', 'rate', 'pm', 'min'
    ]
    
    for col in numeric_cols:
        if col in player_df.columns:
            player_df[col] = pd.to_numeric(player_df[col], errors='coerce')
    
    if 'starter' in player_df.columns:
        player_df['starter'] = pd.to_numeric(player_df['starter'], errors='coerce')
    
    agg_dict = {col: 'mean' for col in numeric_cols if col in player_df.columns}
    agg_dict['game_id'] = 'count'
    
    if 'starter' in player_df.columns:
        agg_dict['starter'] = 'sum'
    
    player_avg = player_df.groupby(['player_name', 'team']).agg(agg_dict).reset_index()
    player_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
    
    if 'starter' in player_avg.columns:
        player_avg.rename(columns={'starter': 'games_started'}, inplace=True)
    
    if '2ptm' in player_avg.columns and '2pta' in player_avg.columns:
        player_avg['2pt_pct'] = (player_avg['2ptm'] / player_avg['2pta']).fillna(0) * 100
    
    if '3ptm' in player_avg.columns and '3pta' in player_avg.columns:
        player_avg['3pt_pct'] = (player_avg['3ptm'] / player_avg['3pta']).fillna(0) * 100
    
    if 'fgm' in player_avg.columns and 'fga' in player_avg.columns:
        player_avg['fg_pct'] = (player_avg['fgm'] / player_avg['fga']).fillna(0) * 100
    
    if 'ftm' in player_avg.columns and 'fta' in player_avg.columns:
        player_avg['ft_pct'] = (player_avg['ftm'] / player_avg['fta']).fillna(0) * 100
    
    player_avg = player_avg.round(1)
    
    desired_order = [
        'player_name', 'team', 'games_played', 'games_started', 'min', 'pts',
        'fgm', 'fga', 'fg_pct',
        '2ptm', '2pta', '2pt_pct',
        '3ptm', '3pta', '3pt_pct',
        'ftm', 'fta', 'ft_pct',
        'def', 'off', 'reb',
        'ast', 'stl', 'to', 'pf', 'pfa',
        'blk', 'blka', 'rate', 'pm'
    ]
    
    player_avg = player_avg[[col for col in desired_order if col in player_avg.columns]]
    player_avg.to_csv(player_averages_file, index=False, encoding='utf-8-sig')
    log_message(f"✅ Player averages calculated")
    log_message(f"   File: {player_averages_file}")
    log_message(f"   Players: {len(player_avg)}")
    
    # TEAM AVERAGES
    numeric_team_cols = [
        'pts', '2ptm', '2pta', '3ptm', '3pta', 'fgm', 'fga',
        'ftm', 'fta', 'def', 'off', 'reb', 'pf', 'pfa',
        'stl', 'to', 'ast', 'blk', 'blka', 'rate',
        'second_chance_pts', 'bench_pts', 'fast_break_pts',
        'points_in_paint', 'pts_off_turnovers'
    ]
    
    for col in numeric_team_cols:
        if col in team_df.columns:
            team_df[col] = pd.to_numeric(team_df[col], errors='coerce')
    
    team_avg = team_df.groupby('team').agg({
        **{col: 'mean' for col in numeric_team_cols if col in team_df.columns},
        'game_id': 'count'
    }).reset_index()
    
    team_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
    
    if all(col in team_avg.columns for col in ['fga', 'fta', 'off', 'to']):
        team_avg['possessions'] = (
            team_avg['fga'] + 
            (0.44 * team_avg['fta']) - 
            team_avg['off'] + 
            team_avg['to']
        ).round(2)
    
    if '2ptm' in team_avg.columns and '2pta' in team_avg.columns:
        team_avg['2pt_pct'] = (team_avg['2ptm'] / team_avg['2pta']).fillna(0) * 100
    
    if '3ptm' in team_avg.columns and '3pta' in team_avg.columns:
        team_avg['3pt_pct'] = (team_avg['3ptm'] / team_avg['3pta']).fillna(0) * 100
    
    if 'fgm' in team_avg.columns and 'fga' in team_avg.columns:
        team_avg['fg_pct'] = (team_avg['fgm'] / team_avg['fga']).fillna(0) * 100
    
    if 'ftm' in team_avg.columns and 'fta' in team_avg.columns:
        team_avg['ft_pct'] = (team_avg['ftm'] / team_avg['fta']).fillna(0) * 100
    
    team_avg = team_avg.round(1)
    
    higher_better_cols = [
        'pts', 'fgm', 'fga', 'fg_pct', '2ptm', '2pta', '2pt_pct',
        '3ptm', '3pta', '3pt_pct', 'ftm', 'fta', 'ft_pct',
        'def', 'off', 'reb', 'ast', 'stl', 'blk', 'pfa', 'rate',
        'second_chance_pts', 'bench_pts', 'fast_break_pts',
        'points_in_paint', 'pts_off_turnovers', 'possessions'
    ]
    
    lower_better_cols = ['to', 'pf', 'blka']
    
    for col in higher_better_cols:
        if col in team_avg.columns:
            rank_col = f"{col}_rank"
            team_avg[rank_col] = team_avg[col].rank(ascending=False, method='min').astype(int)
    
    for col in lower_better_cols:
        if col in team_avg.columns:
            rank_col = f"{col}_rank"
            team_avg[rank_col] = team_avg[col].rank(ascending=True, method='min').astype(int)
    
    final_cols = ['team', 'games_played']
    for col in team_avg.columns:
        if col not in final_cols and not col.endswith('_rank'):
            final_cols.append(col)
            rank_col = f"{col}_rank"
            if rank_col in team_avg.columns:
                final_cols.append(rank_col)
    
    team_avg = team_avg[final_cols]
    
    # OPPONENT AVERAGES
    opponent_stats = []
    
    for game_id in team_df['game_id'].unique():
        game_teams = team_df[team_df['game_id'] == game_id]
        if len(game_teams) == 2:
            team1 = game_teams.iloc[0]
            team2 = game_teams.iloc[1]
            
            opp1 = {'team': team1['team'], 'game_id': game_id}
            opp2 = {'team': team2['team'], 'game_id': game_id}
            
            for col in numeric_team_cols:
                if col in team2:
                    opp1[f"opp_{col}"] = team2[col]
                if col in team1:
                    opp2[f"opp_{col}"] = team1[col]
            
            opponent_stats.append(opp1)
            opponent_stats.append(opp2)
    
    if opponent_stats:
        opp_df = pd.DataFrame(opponent_stats)
        
        opp_cols = [col for col in opp_df.columns if col.startswith('opp_')]
        opponent_avg = opp_df.groupby('team').agg({
            **{col: 'mean' for col in opp_cols},
            'game_id': 'count'
        }).reset_index()
        
        opponent_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
        
        if 'opp_2ptm' in opponent_avg.columns and 'opp_2pta' in opponent_avg.columns:
            opponent_avg['opp_2pt_pct'] = (opponent_avg['opp_2ptm'] / opponent_avg['opp_2pta']).fillna(0) * 100
        
        if 'opp_3ptm' in opponent_avg.columns and 'opp_3pta' in opponent_avg.columns:
            opponent_avg['opp_3pt_pct'] = (opponent_avg['opp_3ptm'] / opponent_avg['opp_3pta']).fillna(0) * 100
        
        if 'opp_fgm' in opponent_avg.columns and 'opp_fga' in opponent_avg.columns:
            opponent_avg['opp_fg_pct'] = (opponent_avg['opp_fgm'] / opponent_avg['opp_fga']).fillna(0) * 100
        
        if 'opp_ftm' in opponent_avg.columns and 'opp_fta' in opponent_avg.columns:
            opponent_avg['opp_ft_pct'] = (opponent_avg['opp_ftm'] / opponent_avg['opp_fta']).fillna(0) * 100
        
        if all(col in opponent_avg.columns for col in ['opp_fga', 'opp_fta', 'opp_off', 'opp_to']):
            opponent_avg['opp_possessions'] = (
                opponent_avg['opp_fga'] + 
                (0.44 * opponent_avg['opp_fta']) - 
                opponent_avg['opp_off'] + 
                opponent_avg['opp_to']
            ).round(2)
        
        opponent_avg = opponent_avg.round(1)
        
        cols_to_drop = ['opp_bench_pts', 'opp_pfa']
        for col in cols_to_drop:
            if col in opponent_avg.columns:
                opponent_avg = opponent_avg.drop(col, axis=1)
        
        opp_stat_cols = [col for col in opponent_avg.columns if col.startswith('opp_') and col != 'opp_to']
        
        for col in opp_stat_cols:
            rank_col = f"{col}_rank"
            opponent_avg[rank_col] = opponent_avg[col].rank(ascending=True, method='min').astype(int)
        
        if 'opp_to' in opponent_avg.columns:
            opponent_avg['opp_to_rank'] = opponent_avg['opp_to'].rank(ascending=False, method='min').astype(int)
        
        final_opp_cols = ['team', 'games_played']
        for col in opponent_avg.columns:
            if col not in final_opp_cols and not col.endswith('_rank'):
                final_opp_cols.append(col)
                rank_col = f"{col}_rank"
                if rank_col in opponent_avg.columns:
                    final_opp_cols.append(rank_col)
        
        opponent_avg = opponent_avg[final_opp_cols]
        opponent_avg.to_csv(opponent_averages_file, index=False, encoding='utf-8-sig')
        log_message(f"✅ Opponent averages calculated")
        log_message(f"   File: {opponent_averages_file}")
        log_message(f"   Teams: {len(opponent_avg)}")
        
        if 'opp_pts' in opponent_avg.columns:
            opp_pts = opponent_avg[['team', 'opp_pts', 'opp_pts_rank']].copy()
            opp_pts.rename(columns={
                'opp_pts': 'pts_allowed',
                'opp_pts_rank': 'pts_allowed_rank'
            }, inplace=True)
            
            team_avg = pd.merge(team_avg, opp_pts, on='team', how='left')
            
            cols = team_avg.columns.tolist()
            if 'pts' in cols:
                pts_idx = cols.index('pts')
                if 'pts_rank' in cols:
                    pts_idx = cols.index('pts_rank')
                
                cols.remove('pts_allowed')
                cols.remove('pts_allowed_rank')
                cols.insert(pts_idx + 1, 'pts_allowed')
                cols.insert(pts_idx + 2, 'pts_allowed_rank')
                team_avg = team_avg[cols]
    
    team_avg.to_csv(team_averages_file, index=False, encoding='utf-8-sig')
    log_message(f"✅ Team averages calculated")
    log_message(f"   File: {team_averages_file}")
    log_message(f"   Teams: {len(team_avg)}")
    
    return True

# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """Main execution - runs all updates automatically"""
    log_message("")
    log_message("="*60)
    log_message("LEUMIT LEAGUE AUTO-UPDATE STARTED")
    log_message("="*60)
    
    # Verify file paths
    log_message("\n📁 Expected output files:")
    log_message(f"   {DATA_FOLDER}/leumit_player_details.csv")
    log_message(f"   {DATA_FOLDER}/leumit_player_history.csv")
    log_message(f"   {DATA_FOLDER}/leumit_player_averages.csv")
    log_message(f"   {DATA_FOLDER}/leumit_team_averages.csv")
    log_message(f"   {DATA_FOLDER}/leumit_opponent_averages.csv")
    log_message(f"   {GAMES_FOLDER}/games_schedule.csv")
    log_message(f"   {GAMES_FOLDER}/game_quarters.csv")
    log_message(f"   {GAMES_FOLDER}/game_player_stats.csv")
    log_message(f"   {GAMES_FOLDER}/game_team_stats.csv")
    
    try:
        # Step 1: Update player details
        if not update_player_details():
            log_message("❌ Player update failed")
            return
        
        # Step 2: Update game details
        if not update_game_details():
            log_message("❌ Game update failed")
            return
        
        # Step 3: Calculate averages
        if not calculate_averages():
            log_message("❌ Averages calculation failed")
            return
        
        log_message("")
        log_message("="*60)
        log_message("✅ ALL UPDATES COMPLETED SUCCESSFULLY")
        log_message("="*60)
        log_message("\n📊 Summary of updated files:")
        
        # List all created files with their sizes
        all_files = [
            f"{DATA_FOLDER}/leumit_player_details.csv",
            f"{DATA_FOLDER}/leumit_player_history.csv",
            f"{DATA_FOLDER}/leumit_player_averages.csv",
            f"{DATA_FOLDER}/leumit_team_averages.csv",
            f"{DATA_FOLDER}/leumit_opponent_averages.csv",
            f"{GAMES_FOLDER}/games_schedule.csv",
            f"{GAMES_FOLDER}/game_quarters.csv",
            f"{GAMES_FOLDER}/game_player_stats.csv",
            f"{GAMES_FOLDER}/game_team_stats.csv"
        ]
        
        for filepath in all_files:
            if os.path.exists(filepath):
                size_kb = os.path.getsize(filepath) / 1024
                log_message(f"   ✓ {filepath} ({size_kb:.1f} KB)")
            else:
                log_message(f"   ✗ {filepath} (not found)")
        
    except Exception as e:
        log_message(f"❌ CRITICAL ERROR: {e}")
        import traceback
        log_message(traceback.format_exc())

if __name__ == "__main__":
    main()


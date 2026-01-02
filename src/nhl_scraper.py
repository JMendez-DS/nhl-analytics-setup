import requests
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict
import os

# --- CONFIG ---
TEAMS_TO_WATCH = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", 
    "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", 
    "OTT", "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "UTA", "VAN", 
    "VGK", "WSH", "WPG"
]
REFRESH_RATE_MINUTES = 5          

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "nhl_leaders_live.csv") 

# Improvement: Added 'session' parameter to reuse connections
def get_team_stats(team_abbr: str, session: requests.Session) -> List[Dict]:
    """
    Fetches real-time roster stats using a shared session.
    """
    url = f"https://api-web.nhle.com/v1/club-stats/{team_abbr}/now"
    
    try:
        # Use session instead of requests.get
        response = session.get(url, timeout=10)
        
        # Specific check for Rate Limiting (429)
        if response.status_code == 429:
            print(f"Rate limit hit for {team_abbr}. Waiting 5 seconds...")
            time.sleep(5)
            return []

        response.raise_for_status() 
        data = response.json()
        
        processed_players = []
        for player in data.get('skaters', []):
            processed_players.append({
                'Team': team_abbr,
                'Player': f"{player['firstName']['default']} {player['lastName']['default']}",
                'Position': player.get('positionCode', 'N/A'),
                # We use 0 here because the vectorization handles the math safely
                'GamesPlayed': player.get('gamesPlayed', 0), 
                'Goals': player.get('goals', 0),
                'Assists': player.get('assists', 0),
                'Points': player.get('points', 0),
                'Shots': player.get('shots', 0),
                'PlusMinus': player.get('plusMinus', 0),
                'PIM': player.get('penaltyMinutes', 0),
                'GWG': player.get('gameWinningGoals', 0),
                'PPG': player.get('powerPlayGoals', 0),
                'SHG': player.get('shorthandedGoals', 0)
            })
        return processed_players
        
    except requests.exceptions.RequestException as e:
        print(f"Network error with {team_abbr}: {e}")
        return []

def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds calculated metrics using vectorized Pandas operations.
    """
    df['Pts_Per_Game'] = (df['Points'] / df['GamesPlayed']).fillna(0).round(2)
    df['Shooting_Pct'] = (df['Goals'] / df['Shots'] * 100).fillna(0).round(1)
    
    df['Goal_Contribution_Pct'] = (df['Goals'] / df['Points'] * 100).fillna(0).round(1)
    df['Assists_Per_Game'] = (df['Assists'] / df['GamesPlayed']).fillna(0).round(2)
    df['Shots_Per_Game'] = (df['Shots'] / df['GamesPlayed']).fillna(0).round(2)
    
    df['Last_Update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

def main():
    print(f" NHL Scraper Live - Tracking: {len(TEAMS_TO_WATCH)} teams")
    print("   (Ctrl + C to stop)")

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Use a context manager for the session
    with requests.Session() as session:
        # Standardize headers for the entire session
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        while True:
            all_data = []
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{timestamp}] Ping NHL API...")

            for team in TEAMS_TO_WATCH:
                # Pass the session to the helper function
                team_data = get_team_stats(team, session)
                if team_data:
                    all_data.extend(team_data)
                    print(f"   -> {team}: Got {len(team_data)} skaters")

            if all_data:
                df = pd.DataFrame(all_data)
                df_enriched = enrich_data(df)
                
                # Improvement: Organized columns for better data analysis visibility
                cols_order = [
                    'Player', 'Team', 'Position', 'GamesPlayed', 'Points', 
                    'Goals', 'Assists', 'Pts_Per_Game', 'Shooting_Pct', 
                    'PlusMinus', 'PIM', 'GWG', 'PPG', 'SHG',
                    'Goal_Contribution_Pct', 'Assists_Per_Game', 'Shots_Per_Game', 
                    'Last_Update'
                ]
                
                df_final = df_enriched[cols_order].sort_values(by='Points', ascending=False)

                try:
                    df_final.to_csv(OUTPUT_FILE, index=False)
                    leader = df_final.iloc[0]
                    print(f"Data Saved. Leader: {leader['Player']} ({leader['Team']}) - {leader['Points']} pts")
                    
                except PermissionError:
                    print("ERROR: Close the CSV file! Cannot write while open.")
            
            else:
                print("No data received.")

            print(f"Waiting {REFRESH_RATE_MINUTES} minutes for next update...")
            time.sleep(REFRESH_RATE_MINUTES * 60)

if __name__ == "__main__":
    main()
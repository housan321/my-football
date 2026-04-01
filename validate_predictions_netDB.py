"""
OPTIMIZED VALIDATION SCRIPT - Query PENDING from Supabase
Validates all PENDING match results from Supabase and syncs back
No CSV required - uses Supabase as single source of truth
"""

import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import warnings
import json
import os
import math
from supabase import create_client
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# ==================== SUPABASE CONFIGURATION ====================
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
# ==================== API CONFIGURATION ====================
API_KEY = os.getenv("FOOTYSTATSAPI")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env file")

# Primary table name
TABLE_NAME = 'predictions_soccer_v2_ourmodel'

# Try multiple API endpoint configurations
API_CONFIGS = [
    {"url": "https://api.football-data-api.com/match", "param": "match_id"},
    {"url": "https://api.footystats.org/match", "param": "id"},
    {"url": "https://api.footystats.org/match", "param": "match_id"},
]

print("\n" + "=" * 80)
print("AGILITY FOOTBALL PREDICTIONS - SUPABASE-DRIVEN VALIDATION")
print("=" * 80)
print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")


# ==================== SUPABASE CLIENT ====================
class SupabaseValidator:
    def __init__(self, url, key):
        self.supabase = create_client(url, key)
        # Configure timeout if available
        if hasattr(self.supabase, 'postgrest'):
            self.supabase.postgrest.timeout = 60.0

    def fetch_pending_matches(self):
        """Fetch all PENDING matches from Supabase"""
        try:
            response = self.supabase.table(TABLE_NAME) \
                .select(
                "match_id,home_team,away_team,ou_prediction,ml_prediction,over_2_5_odds,under_2_5_odds,home_win_odds,away_win_odds,status,date") \
                .eq('status', 'PENDING') \
                .order('date', desc=True) \
                .execute()

            if not response.data:
                return pd.DataFrame()

            df = pd.DataFrame(response.data)
            print(f"✓ Found {len(df)} PENDING matches to validate")
            if len(df) > 0:
                print(f"✓ Date range: {df['date'].min()} to {df['date'].max()}")
            return df
        except Exception as e:
            print(f"✗ Error fetching PENDING matches: {e}")
            raise

    def update_match_result(self, match_id, update_data):
        """Update a match result in Supabase"""
        try:
            # Convert NaN/None values to None for JSON compliance
            for key, value in update_data.items():
                if isinstance(value, float) and (math.isnan(value) or not math.isfinite(value)):
                    update_data[key] = None
                elif pd.isna(value):
                    update_data[key] = None

            response = self.supabase.table(TABLE_NAME) \
                .update(update_data) \
                .eq('match_id', match_id) \
                .execute()
            return response.data
        except Exception as e:
            print(f"⚠ Error updating Supabase for {match_id}: {str(e)[:100]}")
            raise

    def get_accuracy_metrics(self):
        """Get overall accuracy metrics from SETTLED matches"""
        try:
            response = self.supabase.table(TABLE_NAME) \
                .select("ou_correct,ml_correct,ou_pnl,ml_pnl") \
                .eq('status', 'SETTLED') \
                .execute()

            if not response.data:
                return None

            df = pd.DataFrame(response.data)
            total = len(df)
            ou_correct_count = df['ou_correct'].sum() if 'ou_correct' in df else 0
            ml_correct_count = df['ml_correct'].sum() if 'ml_correct' in df else 0
            total_ou_pnl = df['ou_pnl'].sum() if 'ou_pnl' in df else 0.0
            total_ml_pnl = df['ml_pnl'].sum() if 'ml_pnl' in df else 0.0

            return {
                'total': total,
                'ou_correct_count': ou_correct_count,
                'ml_correct_count': ml_correct_count,
                'total_ou_pnl': total_ou_pnl,
                'total_ml_pnl': total_ml_pnl
            }
        except Exception as e:
            print(f"⚠ Could not retrieve accuracy metrics: {e}")
            return None


print("\n[1/4] Connecting to Supabase...")
print("=" * 80)

try:
    validator = SupabaseValidator(SUPABASE_URL, SUPABASE_KEY)
    print("✓ Connected to Supabase")
except Exception as e:
    print(f"✗ CRITICAL: Cannot connect to Supabase: {e}")
    exit(1)

# ==================== FETCH PENDING MATCHES ====================
print("\n[2/4] Fetching PENDING matches from Supabase...")
print("=" * 80)

try:
    predictions_df = validator.fetch_pending_matches()
    if len(predictions_df) == 0:
        print("ℹ No PENDING matches found in database")
        exit(0)
except Exception as e:
    print(f"✗ Error: {e}")
    exit(1)

# ==================== TEST API FIRST ====================
print("\n[3/4] Testing API configurations...")
print("=" * 80)

working_api_config = None
test_match_id = int(predictions_df.iloc[0]['match_id'])

print(f"Testing with match ID: {test_match_id}\n")

for i, config in enumerate(API_CONFIGS, 1):
    try:
        url = f"{config['url']}?key={API_KEY}&{config['param']}={test_match_id}"
        print(f"[{i}/{len(API_CONFIGS)}] Testing: {config['url']} with {config['param']}=...")

        response = requests.get(config['url'],
                                params={'key': API_KEY, config['param']: test_match_id},
                                timeout=30)

        if response.status_code == 200 and response.text:
            try:
                data = response.json()
                if data.get('success') and data.get('data'):
                    print(f"✓ SUCCESS! This configuration works")
                    working_api_config = config
                    break
                else:
                    print(f"✗ API returned success=false")
            except:
                print(f"✗ Invalid JSON")
        else:
            print(f"✗ HTTP {response.status_code}")

    except Exception as e:
        print(f"✗ Error: {str(e)[:50]}")

    time.sleep(0.3)

if not working_api_config:
    print(f"\n✗ ERROR: No working API configuration found!")
    print(f"\n💡 SOLUTIONS:")
    print(f"   1. Your match IDs ({test_match_id}) are not compatible with these APIs")
    print(f"   2. Check if match IDs are from a different source (RapidAPI, etc.)")
    print(f"   3. Verify your API key has access to match data")
    print(f"   4. The matches might be too old or not yet in the API")
    exit(1)

print(f"\n✓ Using: {working_api_config['url']} with parameter '{working_api_config['param']}'")

# ==================== FETCH & UPDATE ====================
print("\n[4/4] Fetching match results and updating Supabase...")
print("=" * 80)

successful_updates = 0
failed_fetches = 0

for idx, row in predictions_df.iterrows():
    match_id = int(row['match_id'])

    # Get prediction data from database
    predicted_ou = row['ou_prediction']
    predicted_winner = row['ml_prediction']

    # Get odds data from database
    odds_over = float(row['over_2_5_odds']) if row['over_2_5_odds'] else 0
    odds_under = float(row['under_2_5_odds']) if row['under_2_5_odds'] else 0
    odds_home = float(row['home_win_odds']) if row['home_win_odds'] else 0
    odds_away = float(row['away_win_odds']) if row['away_win_odds'] else 0

    home_team = row['home_team']
    away_team = row['away_team']

    try:
        # Fetch match details using working config
        response = requests.get(
            working_api_config['url'],
            params={'key': API_KEY, working_api_config['param']: match_id},
            timeout=30
        )

        if response.status_code == 200 and response.text:
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"✗ {match_id}: JSON error")
                failed_fetches += 1
                continue

            if data.get('success') and data.get('data'):
                match_data = data['data']
                status = match_data.get('status', '')

                if status == 'complete':
                    # Get scores
                    home_score = int(match_data.get('homeGoalCount', 0))
                    away_score = int(match_data.get('awayGoalCount', 0))
                    total_goals = home_score + away_score

                    # Determine winner (for ml_actual)
                    if home_score > away_score:
                        actual_winner = 'Home Win'
                    elif away_score > home_score:
                        actual_winner = 'Away Win'
                    else:
                        actual_winner = 'Draw'

                    # Determine O/U (for ou_actual)
                    actual_over_under = 'Over 2.5' if total_goals > 2.5 else 'Under 2.5'

                    # Calculate correctness
                    ou_correct = 1 if predicted_ou == actual_over_under else 0
                    ml_correct = 1 if predicted_winner == actual_winner else 0

                    # Calculate P/L for Over/Under (ou_pnl)
                    if 'Over' in str(predicted_ou):
                        ou_pnl = round(odds_over - 1, 2) if total_goals > 2.5 else -1.0
                    else:
                        ou_pnl = round(odds_under - 1, 2) if total_goals <= 2.5 else -1.0

                    # Calculate P/L for Winner (ml_pnl)
                    if predicted_winner == 'Home Win':
                        ml_pnl = round(odds_home - 1, 2) if actual_winner == 'Home Win' else -1.0
                    elif predicted_winner == 'Away Win':
                        ml_pnl = round(odds_away - 1, 2) if actual_winner == 'Away Win' else -1.0
                    elif predicted_winner == 'Draw':
                        ml_pnl = round(0 - 1, 2) if actual_winner == 'Draw' else -1.0
                    else:
                        ml_pnl = 0.0

                    # Prepare update data
                    update_data = {
                        'ml_actual': actual_winner,
                        'ou_actual': actual_over_under,
                        'home_goals': home_score,
                        'away_goals': away_score,
                        'total_goals': total_goals,
                        'ou_correct': ou_correct,
                        'ml_correct': ml_correct,
                        'ou_pnl': ou_pnl,
                        'ml_pnl': ml_pnl,
                        'status': 'SETTLED',
                        'updated_at': datetime.utcnow().isoformat()
                    }

                    # Update Supabase
                    validator.update_match_result(match_id, update_data)
                    successful_updates += 1

                    print(f"✓ {match_id}: {home_team} {home_score}-{away_score} {away_team}")
                    print(f"  → Winner: {actual_winner} (Predicted: {predicted_winner}) {'✓' if ml_correct else '✗'}")
                    print(f"  → O/U: {actual_over_under} (Predicted: {predicted_ou}) {'✓' if ou_correct else '✗'}")
                    print(f"  → P/L: O/U=${ou_pnl:.2f} | ML=${ml_pnl:.2f}")

                else:
                    print(f"⏳ {match_id}: Not complete (status: {status})")
                    failed_fetches += 1
            else:
                print(f"⚠ {match_id}: No data")
                failed_fetches += 1
        else:
            print(f"✗ {match_id}: HTTP {response.status_code}")
            failed_fetches += 1

        time.sleep(0.25)

    except Exception as e:
        print(f"✗ {match_id}: {str(e)[:80]}")
        failed_fetches += 1

# ==================== SUMMARY ====================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✓ Successfully updated: {successful_updates} matches")
print(f"✗ Failed/Pending: {failed_fetches} matches")
print(f"📊 Total PENDING checked: {len(predictions_df)} matches")

if successful_updates > 0:
    # Calculate accuracy from Supabase
    metrics = validator.get_accuracy_metrics()
    if metrics and metrics['total'] > 0:
        total = metrics['total']
        ou_correct_count = metrics['ou_correct_count']
        ml_correct_count = metrics['ml_correct_count']
        total_ou_pnl = metrics['total_ou_pnl']
        total_ml_pnl = metrics['total_ml_pnl']

        print(f"\n📊 OVERALL ACCURACY METRICS (ALL SETTLED):")
        print(f"   O/U Accuracy: {ou_correct_count}/{total} ({100 * ou_correct_count / total:.1f}%)")
        print(f"   ML Accuracy: {ml_correct_count}/{total} ({100 * ml_correct_count / total:.1f}%)")
        print(f"\n💰 OVERALL PROFIT/LOSS (ALL SETTLED):")
        print(f"   O/U P/L: ${total_ou_pnl:.2f}")
        print(f"   ML P/L: ${total_ml_pnl:.2f}")
        print(f"   Total P/L: ${total_ou_pnl + total_ml_pnl:.2f}")

if successful_updates == 0:
    print(f"\n⚠️ WARNING: No matches were successfully validated")
    print(f"   This suggests the match IDs are incompatible with the API")

print("\n" + "=" * 80)
print("✅ VALIDATION COMPLETE - Supabase Updated!")
print("=" * 80)
print(f"⏰ Completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("=" * 80)
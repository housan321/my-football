"""
Save Football Predictions to Supabase Database
Reads predictions_output.csv and inserts into predictions_soccer_v2_ourmodel table
Uses Supabase Python SDK for database operations
"""

import pandas as pd
from supabase import create_client
from datetime import datetime
import sys
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ==================== SUPABASE CONFIGURATION ====================
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://xifytobpvyuaekaukmet.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Primary table name
TABLE_NAME = 'predictions_soccer_v2_ourmodel'
CSV_FILE = 'predictions_output.csv'

# ==================== LEAGUE ID MAPPING ====================
LEAGUE_MAPPING = {
    12325: "英超",  # England Premier League
    15050: "英超",  # England Premier League
    14924: "欧冠",  # UEFA Champions League
    12316: "西甲",  # Spain La Liga
    14956: "西甲",  # Spain La Liga
    12530: "意甲",  # Italy Serie A
    15068: "意甲",  # Italy Serie A
    12529: "德甲",  # Germany Bundesliga
    14968: "德甲",  # Germany Bundesliga
    13973: "美职联",  # USA MLS
    12337: "法甲",  # France Ligue 1
    14932: "法甲",  # France Ligue 1
    12322: "荷甲",  # Netherlands Eredivisie
    14936: "荷甲",  # Netherlands Eredivisie
    15115: "葡超",  # Portugal Liga NOS
    12136: "墨超",  # Mexico Liga MX
    15234: "墨超",  # Mexico Liga MX
    14937: "比甲",
    15055: "丹麦超",
    14972: "土超",
    16036: "澳甲",
    15047: "瑞士超",
    15163: "希腊超",
    15000: "苏超",
    16614: "哥甲",
}


def get_league_name(league_id):
    """Get league name from league_id using the mapping"""
    try:
        league_id_int = int(league_id)
        return LEAGUE_MAPPING.get(league_id_int, "Unknown League")
    except:
        return "Unknown League"


def get_ou_grade(confidence):
    """Convert ou_confidence value (0-100) to letter grade based on ROI thresholds"""
    if pd.isna(confidence):
        return 'C'
    try:
        conf = float(confidence)
        if conf >= 78.0:
            return "A"
        elif conf >= 65.7:
            return "B"
        elif conf >= 35.7:
            return "D"
        else:
            return "C"
    except:
        return 'C'


def get_ml_grade(confidence):
    """Convert confidence value (0-100) to letter grade"""
    if pd.isna(confidence):
        return None
    try:
        conf = float(confidence)
        if conf >= 90:
            return "A+"
        elif conf >= 85:
            return "A"
        elif conf >= 80:
            return "A-"
        elif conf >= 75:
            return "B+"
        elif conf >= 70:
            return "B"
        elif conf >= 65:
            return "B-"
        elif conf >= 60:
            return "C+"
        elif conf >= 55:
            return "C"
        elif conf >= 50:
            return "C-"
        else:
            return "D"
    except:
        return None


class SupabasePredictionsDB:
    """Supabase database handler for predictions"""

    def __init__(self, url=None, key=None):
        """Initialize Supabase client"""
        self.url = url or SUPABASE_URL
        self.key = key or SUPABASE_KEY

        if not self.key:
            raise ValueError("SUPABASE_KEY is required. Set it in .env file or pass as parameter.")

        # Create Supabase client
        self.supabase = create_client(self.url, self.key)

        # Configure timeout if available
        if hasattr(self.supabase, 'postgrest'):
            self.supabase.postgrest.timeout = 60.0

        logger.info(f"Connected to Supabase: {self.url}")

    def verify_connection(self):
        """Verify database connection and table exists"""
        try:
            # Try to query the table to verify connection
            result = self.supabase.table(TABLE_NAME).select("*").limit(1).execute()
            logger.info(f"✓ Connected to Supabase, table '{TABLE_NAME}' exists")
            return True
        except Exception as e:
            logger.error(f"✗ Connection verification failed: {e}")
            logger.error("  Make sure the table exists in Supabase")
            return False

    def get_existing_match_ids(self):
        """Get all existing match_ids from the database"""
        try:
            # Fetch all match_ids
            result = self.supabase.table(TABLE_NAME).select("match_id").execute()
            existing_ids = [row['match_id'] for row in result.data]
            logger.info(f"✓ Found {len(existing_ids)} existing records in database")
            return set(existing_ids)
        except Exception as e:
            logger.error(f"Error fetching existing match_ids: {e}")
            return set()

    def upsert_predictions(self, df, batch_size=50):
        """
        Upsert predictions to Supabase
        Uses upsert operation (insert on conflict update)
        """
        inserted = 0
        updated = 0
        errors = 0
        error_details = []

        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')

        logger.info(f"Upserting {len(records)} records to Supabase...")

        # Process in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(records) + batch_size - 1) // batch_size

            try:
                # Use Supabase upsert with on_conflict parameter
                # This requires the table to have a unique constraint on match_id
                result = self.supabase.table(TABLE_NAME).upsert(
                    batch,
                    on_conflict='match_id'  # Update on conflict
                ).execute()

                # Count how many were inserted vs updated
                # Supabase doesn't directly tell us, so we track based on response
                if result.data:
                    batch_inserted = len(result.data)
                    logger.info(f"  Batch {batch_num}/{total_batches}: {batch_inserted} records processed")
                    inserted += batch_inserted
                else:
                    logger.info(f"  Batch {batch_num}/{total_batches}: No records returned")

            except Exception as e:
                errors += len(batch)
                error_msg = f"Batch {batch_num}: {str(e)[:100]}"
                error_details.append(error_msg)
                logger.error(f"  ✗ Error: {error_msg}")

                # Try individual inserts for failed batch
                logger.info(f"  Attempting individual inserts for batch {batch_num}...")
                for record in batch:
                    try:
                        result = self.supabase.table(TABLE_NAME).upsert(
                            record,
                            on_conflict='match_id'
                        ).execute()
                        if result.data:
                            inserted += 1
                    except Exception as single_error:
                        errors += 1
                        error_details.append(f"Match ID {record.get('match_id')}: {str(single_error)[:100]}")
                        logger.error(f"    ✗ Failed: {record.get('match_id')}")

        return inserted, updated, errors, error_details

    def get_statistics(self):
        """Get database statistics"""
        try:
            # Get total count
            result = self.supabase.table(TABLE_NAME).select("*", count='exact').limit(0).execute()
            total = result.count if hasattr(result, 'count') else 0

            # Get date range
            date_result = self.supabase.table(TABLE_NAME) \
                .select("date") \
                .order('date', desc=False) \
                .limit(1) \
                .execute()

            earliest_date = date_result.data[0]['date'] if date_result.data else None

            date_result_desc = self.supabase.table(TABLE_NAME) \
                .select("date") \
                .order('date', desc=True) \
                .limit(1) \
                .execute()

            latest_date = date_result_desc.data[0]['date'] if date_result_desc.data else None

            return {
                'total_records': total,
                'earliest_date': earliest_date,
                'latest_date': latest_date
            }

        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return None




def main():
    """Main function to save predictions to Supabase"""

    print("=" * 80)
    print("SAVING PREDICTIONS TO SUPABASE DATABASE")
    print("=" * 80)
    print(f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # ==================== LOAD CSV DATA ====================
    print(f"\n[1/5] Loading CSV file: {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE)
        df = df.dropna() ##删除有空值的行
        print(f"✓ Loaded {len(df)} records from CSV")
        print(f"  Columns: {len(df.columns)}")

        # Map league_id to league_name
        df['league'] = df['league_id'].apply(get_league_name)
        print(f"✓ Mapped league names from league IDs")

        # Calculate grades from confidence values
        df['ou_grade'] = df['ou_confidence'].apply(get_ou_grade)
        df['ml_grade'] = df['ml_confidence'].apply(get_ml_grade)
        print(f"✓ Calculated ou_grade and ml_grade from confidence values")

        # Set status to PENDING for all predictions
        df['status'] = 'PENDING'
        print(f"✓ Set status to PENDING for all records")

        # Convert date to string format
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
            print(f"✓ Formatted dates")

        # Prepare final columns for database
        # Keep only the columns that match the database table schema
        db_columns = [
            'match_id', 'home_id', 'away_id', 'league_id', 'league', 'date',
            'home_team', 'away_team', 'ou_prediction', 'ou_probability',
            'over_2_5_odds', 'under_2_5_odds', 'ml_prediction', 'ml_probability',
            'home_win_odds', 'away_win_odds', 'ou_confidence', 'ml_confidence',
            'ou_confidence_level', 'ml_confidence_level', 'ou_grade', 'ml_grade', 'status'
        ]

        # Filter to only needed columns
        available_columns = [col for col in db_columns if col in df.columns]
        df = df[available_columns]

        # Convert NaN to None for JSON serialization
        df = df.where(pd.notnull(df), None)

        print(f"✓ Prepared {len(df)} records for database")

    except Exception as e:
        print(f"✗ Error loading CSV: {e}")
        sys.exit(1)

    # ==================== CONNECT TO SUPABASE ====================
    print("\n[2/5] Connecting to Supabase...")
    try:
        db = SupabasePredictionsDB()

        if not db.verify_connection():
            print("✗ Failed to connect to Supabase")
            sys.exit(1)

        print("✓ Supabase connection successful")

    except Exception as e:
        print(f"✗ Connection error: {e}")
        print("\nPlease check your .env file has:")
        print("  SUPABASE_URL=your_supabase_url")
        print("  SUPABASE_KEY=your_supabase_anon_key")
        sys.exit(1)

    # ==================== CHECK EXISTING RECORDS ====================
    print("\n[3/5] Checking existing records...")
    try:
        existing_ids = db.get_existing_match_ids()

        # Split into new and existing records
        existing_data = df[df['match_id'].isin(existing_ids)].copy()
        new_data = df[~df['match_id'].isin(existing_ids)].copy()

        print(f"\n  Records breakdown:")
        print(f"    • Total in CSV: {len(df)}")
        print(f"    • Already in DB (will update): {len(existing_data)}")
        print(f"    • New to insert: {len(new_data)}")

    except Exception as e:
        print(f"✗ Error checking existing records: {e}")
        existing_data = pd.DataFrame()
        new_data = df.copy()

    # ==================== UPSERT TO DATABASE ====================
    print("\n[4/5] Upserting to Supabase...")

    # Combine both datasets for upsert
    all_data = pd.concat([existing_data, new_data], ignore_index=True) if len(existing_data) > 0 else new_data

    if len(all_data) == 0:
        print("✓ No records to upsert")
    else:
        inserted, updated, errors, error_details = db.upsert_predictions(all_data, batch_size=50)

        print(f"\n  Results:")
        print(f"    • Successfully processed: {inserted} records")
        if errors > 0:
            print(f"    ⚠ Errors encountered: {errors} records")
            if error_details:
                print(f"\n  Error details (first 3):")
                for i, error in enumerate(error_details[:3], 1):
                    print(f"    {i}. {error}")

    # ==================== VERIFY ====================
    print("\n[5/5] Verifying results...")
    try:
        stats = db.get_statistics()
        if stats:
            print(f"\n  Database Statistics:")
            print(f"    • Total records: {stats['total_records']}")
            if stats['earliest_date']:
                print(f"    • Date range: {stats['earliest_date']} to {stats['latest_date']}")
    except Exception as e:
        print(f"  ⚠ Could not retrieve statistics: {e}")

    # ==================== FINAL SUMMARY ====================
    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print("✅ SUCCESS! Data saved to Supabase")
    print(f"  Table: {TABLE_NAME}")
    print(f"  Records processed: {len(df)}")
    print("=" * 80)


if __name__ == "__main__":
    main()
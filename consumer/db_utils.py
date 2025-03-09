from datetime import datetime
import os
import psycopg2
from psycopg2.extras import DictCursor
from typing import Dict, Any, List, Set
from dateutil import parser
from collections import defaultdict
import json
from dotenv import load_dotenv

load_dotenv()

class RegulationNotFoundError(Exception):
    """Raised when a target regulation is not found in the database"""
    pass

class DatabaseManager:
    def __init__(self, host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")):
        self.conn_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password
        }

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def parse_regulation_date(self, date_str: str) -> datetime.date:
        """Parse regulation date from various formats including Indonesian dates"""
        if not date_str:
            return None

        try:
            # First try standard parsing
            return parser.parse(date_str).date()
        except (ValueError, TypeError):
            try:
                # Try parsing Indonesian format (e.g., "30 Desember 2014")
                day, month_indo, year = date_str.strip().split(' ')
                
                # Use the MONTH_MAPPING from vertex.py
                month = {
                    'Januari': '01',
                    'Februari': '02',
                    'Maret': '03',
                    'April': '04',
                    'Mei': '05',
                    'Juni': '06',
                    'Juli': '07',
                    'Agustus': '08',
                    'September': '09',
                    'Oktober': '10',
                    'November': '11',
                    'Desember': '12'
                }.get(month_indo)
                
                if not month:
                    return None
                    
                # Create ISO format date string
                iso_date = f"{year}-{month}-{day.zfill(2)}"
                return datetime.strptime(iso_date, "%Y-%m-%d").date()
            except (ValueError, TypeError, AttributeError):
                return None

    def upsert_regulation(self, regulation_data: Dict[str, Any]) -> int:
        """
        Insert or update a regulation in the database
        Returns the regulation ID
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # Try to find existing regulation by number, year and issuing body
                cur.execute(
                    "SELECT id FROM Regulation WHERE number = %s AND year = %s AND issuing_body = %s",
                    (regulation_data["number"], str(regulation_data["year"]), regulation_data["type"])
                )
                result = cur.fetchone()

                # Parse dates if they're strings
                issuance_date = (
                    self.parse_regulation_date(regulation_data.get("date"))
                    if regulation_data.get("date")
                    else None
                )
                
                effective_date = (
                    self.parse_regulation_date(regulation_data.get("effective_date"))
                    if regulation_data.get("effective_date")
                    else issuance_date  # Use issuance date as effective date if not specified
                )

                if result:
                    # Update existing regulation
                    cur.execute("""
                        UPDATE Regulation 
                        SET title = %s,
                            issuing_body = %s,
                            issuance_date = %s,
                            effective_date = %s,
                            status = %s,
                            source_url = %s,
                            last_updated = NOW()
                        WHERE number = %s AND year = %s AND issuing_body = %s
                        RETURNING id
                    """, (
                        regulation_data["title"],
                        regulation_data["type"].upper(),
                        issuance_date,
                        effective_date,
                        regulation_data.get("status", "BERLAKU"),
                        regulation_data.get("source_url"),
                        regulation_data["number"].upper(),
                        regulation_data["year"],
                        regulation_data["type"].upper()
                    ))
                else:
                    # Insert new regulation
                    cur.execute("""
                        INSERT INTO Regulation (
                            number, year, title, issuing_body, issuance_date, 
                            effective_date, status, source_url, last_updated
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        RETURNING id
                    """, (
                        regulation_data["number"].upper(),
                        regulation_data["year"],
                        regulation_data["title"],
                        regulation_data["type"].upper(),
                        issuance_date,
                        effective_date,
                        regulation_data.get("status", "BERLAKU"),
                        regulation_data.get("source_url")
                    ))

                regulation_id = cur.fetchone()[0]
                conn.commit()
                return regulation_id

def store_regulation(regulation_metadata: Dict[str, Any]) -> bool:
    """
    Store the regulation in the database
    Returns True if processing was successful, False otherwise
    """
    db = DatabaseManager()
    
    try:
        # Process the current regulation with its metadata
        regulation_data = {
            **regulation_metadata,  # Include all metadata
            "status": "BERLAKU",   # Set status as BERLAKU for new regulations
        }
        
        # Insert/update the regulation and get its ID
        regulation_id = db.upsert_regulation(regulation_data)

    except Exception as e:
        error_message = f"Error processing regulation {regulation_metadata.get('number', 'unknown')}: {str(e)}"
        print(error_message)

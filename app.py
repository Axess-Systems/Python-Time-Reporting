import os
import sys
import sqlite3
import argparse
from datetime import datetime, timedelta
import calendar
import pandas as pd
import aiohttp
import asyncio
from dotenv import load_dotenv
import json
from tqdm import tqdm
import time


# Load environment variables from .env file
load_dotenv()


class TicketReporter:
    def __init__(self, start_date=None, end_date=None):
        # Load domain and API key directly from environment variables
        self.domain = os.getenv('FRESHSERVICE_DOMAIN')
        self.api_key = os.getenv('FRESHSERVICE_API_KEY')
        self.time_frame = os.getenv('REPORT_TIME_FRAME', 'last_week')

        if not self.domain or not self.api_key:
            raise ValueError("FRESHSERVICE_DOMAIN and FRESHSERVICE_API_KEY must be set in the .env file.")

        self.base_url = f"https://{self.domain}/api/v2"
        self.rate_limit_delay = 2
        self.max_retries = 3
        self.progress_bar = None
        self.total_tickets = 0

        # Use provided start and end dates if available; otherwise calculate from time frame
        if start_date and end_date:
            self.start_date = start_date
            self.end_date = end_date
        else:
            self.start_date, self.end_date = self.calculate_date_range(self.time_frame)

    def calculate_date_range(self, time_frame):
        """Calculate the start and end dates based on the specified time frame"""
        today = datetime.today()
        if time_frame == "last_week":
            start_date = today - timedelta(days=today.weekday() + 7)
            end_date = start_date + timedelta(days=6)
        elif time_frame == "this_week":
            start_date = today - timedelta(days=today.weekday())
            end_date = start_date + timedelta(days=6)
        elif time_frame == "last_month":
            first_day_last_month = today.replace(day=1) - timedelta(days=1)
            start_date = first_day_last_month.replace(day=1)
            end_date = first_day_last_month
        elif time_frame == "last_3_months":
            month_ago = today.replace(day=1) - timedelta(days=1)
            start_date = (month_ago.replace(day=1) - timedelta(days=month_ago.day + 59)).replace(day=1)
            end_date = month_ago
        else:
            raise ValueError(f"Unsupported time frame: {time_frame}")
        
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


    async def get_total_tickets(self):
        """Get total number of tickets for progress bar"""
        query = f"created_at:>'{self.start_date}' AND created_at:<'{self.end_date}'"
        response = await self.make_api_request(
            f'tickets/filter?query="{query}"&page=1&per_page=1'
        )
        if response and 'total' in response:
            return response['total']
        return 0       
        
    async def initialize_connection(self):
        """Initialize the API session and database connection"""
        self.session = aiohttp.ClientSession()
        self.db_conn = sqlite3.connect('tickets.db')
        await self.setup_database()

    async def setup_database(self):
        """Set up the database tables"""
        cursor = self.db_conn.cursor()
        
        # Create tickets table with merged information
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY,
                subject TEXT,
                department_id INTEGER,
                created_at TEXT,
                status INTEGER,
                company_id INTEGER,
                requester_name TEXT,
                department_name TEXT,
                is_merged BOOLEAN DEFAULT FALSE,
                merged_into_ticket_id INTEGER,
                ticket_type TEXT
            )
        ''')

        # Create time entries table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS time_entries (
                id INTEGER PRIMARY KEY,
                ticket_id INTEGER,
                time_spent TEXT,
                agent_id INTEGER,
                agent_name TEXT,
                note TEXT,
                created_at TEXT,
                dan_challinor FLOAT DEFAULT 0,
                john_middleton FLOAT DEFAULT 0,
                mike_smith FLOAT DEFAULT 0,
                FOREIGN KEY (ticket_id) REFERENCES tickets (id)
            )
        ''')

        # Create departments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS departments (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')

        # Create agents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agents (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT
            )
        ''')

        # Create conversations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY,
                ticket_id INTEGER,
                body_text TEXT,
                created_at TEXT,
                user_id INTEGER,
                incoming BOOLEAN,
                private BOOLEAN,
                FOREIGN KEY (ticket_id) REFERENCES tickets (id)
            )
        ''')

        self.db_conn.commit()

    async def make_api_request(self, endpoint, params=None):
        """Make an authenticated API request with rate limiting and retries"""
        url = f"{self.base_url}/{endpoint}"
        auth = aiohttp.BasicAuth(self.api_key, 'X')  # Use self.api_key instead of self.config['api_key']
        
        for retry in range(self.max_retries):
            try:
                # Add delay before request
                await asyncio.sleep(self.rate_limit_delay)
                
                async with self.session.get(url, auth=auth, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Rate limit hit
                        retry_after = int(response.headers.get('Retry-After', 30))
                        print(f"Rate limit reached. Waiting {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        print(f"Error: Status code {response.status} for URL: {url}")
                        if retry < self.max_retries - 1:
                            wait_time = (retry + 1) * self.rate_limit_delay
                            print(f"Retrying in {wait_time} seconds... (Attempt {retry + 1}/{self.max_retries})")
                            await asyncio.sleep(wait_time)
                        else:
                            return None
                            
            except Exception as e:
                print(f"API request error for {url}: {str(e)}")
                if retry < self.max_retries - 1:
                    wait_time = (retry + 1) * self.rate_limit_delay
                    print(f"Retrying in {wait_time} seconds... (Attempt {retry + 1}/{self.max_retries})")
                    await asyncio.sleep(wait_time)
                else:
                    return None
                       
    async def fetch_departments(self):
        """Fetch departments with progress bar"""
        print("Fetching departments...", end=' ')
        page = 1
        per_page = 30
        total_departments = 0
        
        while True:
            params = {
                'per_page': per_page,
                'page': page
            }
            
            response = await self.make_api_request('departments', params)
            
            if not response or not response.get('departments'):
                break
                
            departments = response['departments']
            cursor = self.db_conn.cursor()
            
            for dept in departments:
                cursor.execute('''
                    INSERT OR REPLACE INTO departments (id, name)
                    VALUES (?, ?)
                ''', (dept['id'], dept['name']))
            
            self.db_conn.commit()
            total_departments += len(departments)
            
            if len(departments) < per_page:
                break
                
            page += 1
            await asyncio.sleep(0.5)
        
        print(f"Done! ({total_departments} departments)")
        
    async def fetch_agents(self):
        """Fetch and store all agents"""
        print("Fetching agents...")
        response = await self.make_api_request('agents')
        if response and 'agents' in response:
            cursor = self.db_conn.cursor()
            for agent in response['agents']:
                cursor.execute('''
                    INSERT OR REPLACE INTO agents (id, name, email)
                    VALUES (?, ?, ?)
                ''', (agent['id'], f"{agent['first_name']} {agent['last_name']}", agent['email']))
            self.db_conn.commit()
            print(f"Stored {len(response['agents'])} agents")

    async def check_merged_status(self, ticket_id):
        """Check if a ticket is merged by examining its conversations"""
        response = await self.make_api_request(f'tickets/{ticket_id}/conversations')
        
        if response and 'conversations' in response:
            cursor = self.db_conn.cursor()
            for conversation in response['conversations']:
                # Store the conversation
                cursor.execute('''
                    INSERT OR REPLACE INTO conversations (
                        id, ticket_id, body_text, created_at, 
                        user_id, incoming, private
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    conversation['id'],
                    ticket_id,
                    conversation.get('body_text', ''),
                    conversation['created_at'],
                    conversation.get('user_id'),
                    conversation.get('incoming', False),
                    conversation.get('private', False)
                ))
                
                body_text = conversation.get('body_text', '')
                if "This ticket is closed and merged into ticket" in body_text:
                    try:
                        merged_into = int(body_text.split("ticket ")[-1].rstrip('.'))
                        return True, merged_into
                    except (ValueError, IndexError):
                        print(f"Warning: Could not extract merged ticket ID from conversation in ticket {ticket_id}")
            
            self.db_conn.commit()
    
        return False, None

    async def fetch_tickets(self):
        """Fetch tickets within the date range with progress bar"""
        print("\nInitializing data...")
        
        # Fetch departments and agents first
        await self.fetch_departments()
        await self.fetch_agents()
        
        # Get total tickets after initialization
        self.total_tickets = await self.get_total_tickets()
        print(f"\nFound {self.total_tickets} tickets in date range")
        
        # Initialize progress bar
        self.progress_bar = tqdm(
            total=self.total_tickets,
            desc="Fetching tickets",
            unit="ticket"
        )

        page = 1
        per_page = 50
        processed_tickets = 0
        
        print("\nProcessing tickets...")
        while processed_tickets < self.total_tickets:
            query = f"created_at:>'{self.start_date}' AND created_at:<'{self.end_date}'"
            response = await self.make_api_request(
                f'tickets/filter?query="{query}"&page={page}&per_page={per_page}'
            )
            
            if not response or not response.get('tickets'):
                break

            tickets = response['tickets']
            await self.store_tickets(tickets)
            
            processed_tickets += len(tickets)
            self.progress_bar.update(len(tickets))
            
            if len(tickets) < per_page:
                break
            
            page += 1
            await asyncio.sleep(0.5)
        
        self.progress_bar.close()
        print(f"\nCompleted processing {processed_tickets} tickets")
                   
    async def fetch_requester_name(self, requester_id):
        """Fetch requester details"""
        response = await self.make_api_request(f'requesters/{requester_id}')
        if response and 'requester' in response:
            requester = response['requester']
            return f"{requester['first_name']} {requester['last_name']}"
        return "Unknown Requester"

    async def store_tickets(self, tickets):
        """Store tickets with minimal progress updates"""
        cursor = self.db_conn.cursor()
        for ticket in tickets:
            try:
                is_merged, merged_into = await self.check_merged_status(ticket['id'])
                requester_name = await self.fetch_requester_name(ticket['requester_id'])
                
                cursor.execute('SELECT name FROM departments WHERE id = ?', (ticket.get('department_id'),))
                dept_result = cursor.fetchone()
                department_name = dept_result[0] if dept_result else "Unknown Department"

                cursor.execute('''
                    INSERT OR REPLACE INTO tickets (
                        id, subject, department_id, created_at, status, 
                        company_id, requester_name, department_name,
                        is_merged, merged_into_ticket_id, ticket_type
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ticket['id'],
                    ticket['subject'],
                    ticket.get('department_id'),
                    ticket['created_at'],
                    ticket['status'],
                    ticket.get('custom_fields', {}).get('cf_company_id'),
                    requester_name,
                    department_name,
                    is_merged,
                    merged_into,
                    ticket.get('type', 'Unknown')
                ))
                
                await self.fetch_time_entries(ticket['id'])
                self.db_conn.commit()
                
            except Exception as e:
                tqdm.write(f"Error processing ticket {ticket['id']}: {str(e)}")
                continue
                  
    async def fetch_time_entries(self, ticket_id):
        """Fetch time entries for a ticket"""
        response = await self.make_api_request(f'tickets/{ticket_id}/time_entries')
        
        if response and 'time_entries' in response:
            cursor = self.db_conn.cursor()
            for entry in response['time_entries']:
                try:
                    # Get agent name from our agents table
                    cursor.execute('SELECT name FROM agents WHERE id = ?', (entry['agent_id'],))
                    agent_result = cursor.fetchone()
                    agent_name = agent_result[0] if agent_result else "Unknown Agent"

                    # Extract custom fields
                    custom_fields = entry.get('custom_fields', {})
                    dan_challinor = float(custom_fields.get('dan_challinor', 0) or 0)
                    john_middleton = float(custom_fields.get('john_middleton', 0) or 0)
                    mike_smith = float(custom_fields.get('mike_smith', 0) or 0)

                    cursor.execute('''
                        INSERT OR REPLACE INTO time_entries (
                            id, ticket_id, time_spent, agent_id, agent_name, note, created_at,
                            dan_challinor, john_middleton, mike_smith
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        entry['id'],
                        ticket_id,
                        entry['time_spent'],
                        entry['agent_id'],
                        agent_name,
                        entry.get('note', ''),
                        entry['created_at'],
                        dan_challinor,
                        john_middleton,
                        mike_smith
                    ))
                except Exception as e:
                    print(f"Error processing time entry for ticket {ticket_id}: {str(e)}")
                    continue
                
            self.db_conn.commit()
            
    def generate_excel_report(self, output_file='ticket_report.xlsx'):
        """Generate Excel report from the collected data"""
        print("Generating Excel report...")

        # Query for detailed report data
        query = '''
            SELECT 
                t.company_id,
                t.id as ticket_id,
                t.ticket_type,
                t.subject,
                t.requester_name,
                t.department_name,
                t.created_at as ticket_created,
                t.status,
                CASE WHEN t.is_merged THEN 'Yes' ELSE 'No' END as merged,
                t.merged_into_ticket_id as merged_into,
                GROUP_CONCAT(DISTINCT te.agent_name) as agents,
                GROUP_CONCAT(te.time_spent) as time_entries,
                GROUP_CONCAT(te.note) as time_entry_notes,
                SUM(CAST(REPLACE(COALESCE(te.time_spent, '0:00'), ':', '.') AS FLOAT)) as total_hours,
                SUM(te.dan_challinor) as dan_challinor_hours,
                SUM(te.john_middleton) as john_middleton_hours,
                SUM(te.mike_smith) as mike_smith_hours
            FROM tickets t
            LEFT JOIN time_entries te ON t.id = te.ticket_id
            GROUP BY t.id
            ORDER BY t.company_id, t.created_at
        '''
        df = pd.read_sql_query(query, self.db_conn)

        # Convert status codes to readable text
        status_map = {
            2: 'Open',
            3: 'Pending',
            4: 'Resolved',
            5: 'Closed',
            7: 'Scheduled service',
            11: 'Pending 3rd Party',
            12: 'Pending Customer'
        }
        df['status'] = df['status'].map(status_map)

        # Format dates
        df['ticket_created'] = pd.to_datetime(df['ticket_created']).dt.strftime('%Y-%m-%d %H:%M')
        df['merged_into'] = df['merged_into'].fillna('')

        # Create Excel writer
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Generate each summary only if there is data for it

            # Summary by company, department, and ticket type
            summary_df = df.groupby(['company_id', 'department_name', 'ticket_type']).agg({
                'ticket_id': 'count',
                'total_hours': 'sum',
                'dan_challinor_hours': 'sum',
                'john_middleton_hours': 'sum',
                'mike_smith_hours': 'sum'
            }).reset_index()
            
            if not summary_df.empty:
                summary_df.columns = [
                    'Company ID', 'Department', 'Ticket Type', 'Total Tickets', 'Total Hours',
                    'Dan Challinor Hours', 'John Middleton Hours', 'Mike Smith Hours'
                ]
                summary_df.to_excel(writer, sheet_name='Summary', index=False)

            # Type-specific summary
            type_summary = df.groupby(['company_id', 'ticket_type']).agg({
                'ticket_id': 'count',
                'total_hours': 'sum'
            }).reset_index()

            if not type_summary.empty:
                type_summary.columns = ['Company ID', 'Ticket Type', 'Total Tickets', 'Total Hours']
                type_summary.to_excel(writer, sheet_name='Type Summary', index=False)

            # Merged tickets summary
            merged_summary = df[df['merged'] == 'Yes'].groupby(['company_id', 'department_name']).agg({
                'ticket_id': 'count',
                'total_hours': 'sum'
            }).reset_index()

            if not merged_summary.empty:
                merged_summary.columns = ['Company ID', 'Department', 'Merged Tickets Count', 'Merged Tickets Hours']
                merged_summary.to_excel(writer, sheet_name='Merged Summary', index=False)

            # Write detailed report
            df.to_excel(writer, sheet_name='Detailed Report', index=False)

            # Format the workbook
            workbook = writer.book
            header_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
            hours_format = workbook.add_format({'num_format': '0.00'})

            # Format each sheet if present
            for sheet_name in writer.sheets:
                sheet = writer.sheets[sheet_name]
                sheet.set_column('A:Z', 15)
                if sheet_name in ['Summary', 'Type Summary', 'Merged Summary']:
                    for col_idx, col_name in enumerate(df.columns):
                        if 'Hours' in col_name:
                            sheet.set_column(col_idx, col_idx, 15, hours_format)
                sheet.autofilter(0, 0, 0, len(df.columns) - 1)
                for col_num, value in enumerate(df.columns.values):
                    sheet.write(0, col_num, value, header_format)

        print(f"Report generated: {output_file}")
  
        
async def main():
    parser = argparse.ArgumentParser(description='Generate ticket report for date range')
    parser.add_argument('-t', nargs=2, metavar=('start_date', 'end_date'),
                        help='Start and end dates in format YYYY/MM/DD (e.g., 2024/10/01)')
    args = parser.parse_args()
    
    # Try to parse dates if provided
    if args.t:
        try:
            start_date = datetime.strptime(args.t[0], '%Y/%m/%d').strftime('%Y-%m-%d')
            end_date = datetime.strptime(args.t[1], '%Y/%m/%d').strftime('%Y-%m-%d')
            print(f"Using custom date range: {start_date} to {end_date}")
        except ValueError:
            print("Invalid date format. Please use YYYY/MM/DD.")
            sys.exit(1)
    else:
        start_date, end_date = None, None
    
    reporter = TicketReporter(start_date, end_date)
    
    try:
        await reporter.initialize_connection()
        await reporter.fetch_tickets()
        reporter.generate_excel_report()
    except Exception as e:
        print(f"Error during report generation: {str(e)}")
        raise
    finally:
        if hasattr(reporter, 'session') and not reporter.session.closed:
            await reporter.session.close()
        if hasattr(reporter, 'db_conn'):
            reporter.db_conn.close()
    
    print("Report generation completed successfully")

if __name__ == '__main__':
    asyncio.run(main())

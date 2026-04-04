import duckdb
import db_config
from loguru import logger
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import box
from pathlib import Path
import argparse
import sys
import io

# Force UTF-8 for Windows console
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def generate_rich_table(df):
    console = Console()
    table = Table(title="Unmapped Staging Backlog", box=box.ROUNDED, header_style="bold magenta", border_style="cyan")
    
    # Add relevant columns with smart truncation/styling
    table.add_column("ID", justify="right", style="dim")
    table.add_column("Term", width=30, overflow="fold")
    table.add_column("Institution", width=25)
    table.add_column("Period", justify="center")
    table.add_column("Value", justify="right")
    table.add_column("Retry", justify="center")
    table.add_column("Flag", justify="center")
    
    for i, row in df.iterrows():
        # Style flag based on review status
        flag_text = "[bold red]REVIEW[/bold red]" if row['requires_human_review'] else "[green]OK[/green]"
        retry_style = "yellow" if row['retry_count'] > 0 else "dim"
        
        table.add_row(
            str(row['staging_id']),
            row['raw_term'],
            row['institution_id'][:22] + (".." if len(row['institution_id']) > 22 else ""),
            str(row['reporting_period']),
            f"{row['raw_value']:,}",
            f"[{retry_style}]{row['retry_count']}[/{retry_style}]",
            flag_text
        )
    
    console.print(table)
    
    # Traceability Footer
    console.print(f"\n[dim]Showing [bold cyan]{len(df)}[/bold cyan] items from staging.[/dim]")
    if df['requires_human_review'].any():
        console.print(f"⚠️  [bold red]{df['requires_human_review'].sum()}[/bold red] items require manual resolution (retry limit reached).")

def generate_html_report(df, output_path="unmapped_dashboard.html"):
    """Creates a visually stunning HTML dashboard for the staging data."""
    
    # Group by Institution for a cleaner report
    html_rows = ""
    for i, row in df.iterrows():
        status_class = "review" if row['requires_human_review'] else ("retrying" if row['retry_count'] > 0 else "new")
        status_label = "NEEDS HUMAN REVIEW" if row['requires_human_review'] else "QUEUED"
        
        html_rows += f"""
        <tr class="{status_class}">
            <td>{row['staging_id']}</td>
            <td class="term">{row['raw_term']}</td>
            <td>{row['institution_id']}</td>
            <td>{row['reporting_period']}</td>
            <td class="value">{row['raw_value']:,}</td>
            <td class="retry">{row['retry_count']}</td>
            <td><span class="badge">{status_label}</span></td>
        </tr>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>FS Factbase | Unmapped Staging</title>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&family=Outfit:wght@500;700&display=swap');
            
            :root {{
                --bg: #0f172a;
                --card-bg: rgba(30, 41, 59, 0.7);
                --accent: #38bdf8;
                --text: #f8fafc;
                --review: #ef4444;
                --retrying: #f59e0b;
                --new: #10b981;
            }}

            body {{
                background-color: var(--bg);
                color: var(--text);
                font-family: 'Inter', sans-serif;
                margin: 0;
                padding: 40px;
                background-image: radial-gradient(circle at top right, #1e293b, #0f172a);
                min-height: 100vh;
            }}

            h1 {{
                font-family: 'Outfit', sans-serif;
                font-size: 2.5rem;
                margin-bottom: 0.5rem;
                background: linear-gradient(to right, #38bdf8, #818cf8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }}

            .stats {{
                display: flex;
                gap: 20px;
                margin-bottom: 30px;
            }}

            .stat-card {{
                background: var(--card-bg);
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255,255,255,0.1);
                padding: 20px;
                border-radius: 16px;
                flex: 1;
            }}

            .stat-val {{ font-size: 2rem; font-weight: 700; color: var(--accent); }}
            .stat-label {{ font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; }}

            table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0 8px;
                margin-top: 20px;
            }}

            th {{
                padding: 15px;
                text-align: left;
                color: #94a3b8;
                font-weight: 600;
                text-transform: uppercase;
                font-size: 0.75rem;
                letter-spacing: 0.05em;
            }}

            tr {{
                background: var(--card-bg);
                backdrop-filter: blur(10px);
                transition: transform 0.2s, background 0.2s;
                border-radius: 12px;
            }}

            tr:hover {{
                transform: scale(1.005);
                background: rgba(30, 41, 59, 1);
                border: 1px solid var(--accent);
            }}

            td {{
                padding: 15px;
                border-top: 1px solid rgba(255,255,255,0.05);
                border-bottom: 1px solid rgba(255,255,255,0.05);
            }}

            td:first-child {{ border-left: 1px solid rgba(255,255,255,0.05); border-radius: 12px 0 0 12px; }}
            td:last-child {{ border-right: 1px solid rgba(255,255,255,0.05); border-radius: 0 12px 12px 0; }}

            .term {{ font-weight: 600; color: #e2e8f0; }}
            .value {{ font-family: monospace; color: var(--accent); text-align: right; }}
            .retry {{ text-align: center; }}

            .badge {{
                padding: 4px 12px;
                border-radius: 20px;
                font-size: 0.7rem;
                font-weight: 700;
                text-transform: uppercase;
            }}

            .review .badge {{ background: rgba(239, 68, 68, 0.2); color: #ef4444; border: 1px solid #ef4444; }}
            .retrying .badge {{ background: rgba(245, 158, 11, 0.2); color: #f59e0b; border: 1px solid #f59e0b; }}
            .new .badge {{ background: rgba(16, 185, 129, 0.2); color: #10b981; border: 1px solid #10b981; }}

        </style>
    </head>
    <body>
        <h1>Unmapped Staging Dashboard</h1>
        <p style="color: #94a3b8">Financial Extraction Backlog & Triage</p>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-val">{len(df)}</div>
                <div class="stat-label">Total Backlog</div>
            </div>
            <div class="stat-card">
                <div class="stat-val">{df['requires_human_review'].sum()}</div>
                <div class="stat-label" style="color: var(--review)">Needs Review</div>
            </div>
            <div class="stat-card">
                <div class="stat-val">{len(df[df['retry_count'] > 0])}</div>
                <div class="stat-label" style="color: var(--retrying)">Retrying</div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Raw Term</th>
                    <th>Institution</th>
                    <th>Period</th>
                    <th style="text-align: right">Value</th>
                    <th style="text-align: center">Retry</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {html_rows}
            </tbody>
        </table>
    </body>
    </html>
    """
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.success(f"Report generated: {output_path}")
    # Try to open it
    try:
        import webbrowser
        webbrowser.open(f"file://{os.path.abspath(output_path)}")
    except:
        pass

def main():
    parser = argparse.ArgumentParser(description="View Unmapped Staging with Rich formatting.")
    parser.add_argument("--html", action="store_true", help="Generate and open a beautiful HTML dashboard.")
    args = parser.parse_args()

    db_path = db_config.get_db_path()
    conn = duckdb.connect(db_path)
    
    query = """
        SELECT 
            staging_id, 
            raw_term, 
            institution_id, 
            reporting_period, 
            raw_value,
            retry_count, 
            requires_human_review,
            source_document,
            source_page_number
        FROM Unmapped_Staging
        ORDER BY staging_id DESC
    """
    
    df = conn.execute(query).df()
    conn.close()
    
    if df.empty:
        logger.info("Unmapped_Staging is empty.")
        return

    if args.html:
        generate_html_report(df)
    else:
        generate_rich_table(df)

if __name__ == "__main__":
    main()

import xml.etree.ElementTree as ET
import os
from pathlib import Path
from loguru import logger
import duckdb
import db_config

class TaxonomyMetadataParser:
    def __init__(self, taxonomy_dir: str):
        self.taxonomy_dir = Path(taxonomy_dir)
        self.concept_to_role = {} # concept_id -> role_id

    def discover_roles(self):
        """
        Parses presentation linkbases to map concepts to financial statement roles.
        Focuses on standard roles:
        - 210000: Balance Sheet
        - 310000 / 320000: Income Statement
        - 410000 / 420000: OCI
        - 510000 / 520000: Equity
        - 610000: Cash Flow
        """
        linkbase_dir = self.taxonomy_dir / "full_ifrs" / "linkbases"
        if not linkbase_dir.exists():
            logger.error(f"Linkbase directory not found: {linkbase_dir}")
            return

        # We look for all 'pre_' (presentation) files
        pre_files = list(linkbase_dir.glob("**/pre_*.xml"))
        logger.info(f"Found {len(pre_files)} presentation linkbase files.")

        for file_path in pre_files:
            self._parse_pre_file(file_path)

        logger.info(f"Mapped {len(self.concept_to_role)} concepts to roles.")

    def _parse_pre_file(self, file_path: Path):
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            return

        ns = {
            'link': 'http://www.xbrl.org/2003/linkbase',
            'xlink': 'http://www.w3.org/1999/xlink'
        }

        # Extract the role from the filename or the roleRef
        # Filenames like pre_ias_1_2025-03-27_role-210000.xml
        filename = file_path.name
        if "_role-" in filename:
            role_id = filename.split("_role-")[-1].replace(".xml", "")
        else:
            return

        # We only care about major financial statement roles for partitioning
        valid_roles = ["210000", "310000", "320000", "410000", "420000", "510000", "520000", "610000"]
        if role_id not in valid_roles:
            return

        # Find all locators (concepts) in this role
        for loc in root.findall('.//link:loc', ns):
            href = loc.get('{http://www.w3.org/1999/xlink}href')
            if href:
                concept_id = href.split('#')[-1]
                # A concept can appear in multiple roles, but we assign it to the first major FS role it appears in
                if concept_id not in self.concept_to_role:
                    self.concept_to_role[concept_id] = role_id

    def update_db_metadata(self, db_path=None):
        """Updates the Core_Metrics table with the discovered statement roles."""
        if db_path is None:
            db_path = db_config.get_db_path()
            
        conn = duckdb.connect(db_path)
        logger.info("Applying taxonomy roles to Core_Metrics...")
        
        updates = [(role, concept_id) for concept_id, role in self.concept_to_role.items()]
        conn.executemany("""
            UPDATE Core_Metrics 
            SET statement_role = ? 
            WHERE metric_id = ?
        """, updates)
                
        conn.close()
        logger.success(f"Successfully applied taxonomy roles to Core_Metrics.")

if __name__ == "__main__":
    parser = TaxonomyMetadataParser("d:/FS Factbase/p02_Database_and_Mapping/IFRSAT-2025")
    parser.discover_roles()
    parser.update_db_metadata()

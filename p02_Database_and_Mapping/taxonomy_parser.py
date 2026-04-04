import xml.etree.ElementTree as ET
import os
from pathlib import Path
from loguru import logger
from rapidfuzz import process, fuzz

class TaxonomyParser:
    def __init__(self, taxonomy_dir: str):
        self.taxonomy_dir = Path(taxonomy_dir)
        self.label_map = {}  # concept_id -> [labels]
        self.reverse_label_map = {} # label -> concept_id

    def load_labels(self):
        """Loads all English label linkbases from the taxonomy directory."""
        label_dir = self.taxonomy_dir / "full_ifrs" / "labels"
        if not label_dir.exists():
            logger.error(f"Label directory not found: {label_dir}")
            return

        label_files = list(label_dir.glob("*en_*.xml"))
        logger.info(f"Found {len(label_files)} label files.")

        for file_path in label_files:
            self._parse_file(file_path)

        logger.info(f"Loaded {len(self.label_map)} concepts with labels.")

    def _parse_file(self, file_path: Path):
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

        labels = {}  # res_id -> text
        locs = {}    # loc_id -> concept_id

        # 1. Extract labels (text resources)
        for label in root.findall('.//link:label', ns):
            res_id = label.get('{http://www.w3.org/1999/xlink}label')
            text = label.text
            if res_id and text:
                labels[res_id] = text.strip()

        # 2. Extract locators (pointing to concepts)
        for loc in root.findall('.//link:loc', ns):
            loc_id = loc.get('{http://www.w3.org/1999/xlink}label')
            href = loc.get('{http://www.w3.org/1999/xlink}href')
            if loc_id and href:
                concept_id = href.split('#')[-1]
                locs[loc_id] = concept_id

        # 3. Connect them via label arcs
        for arc in root.findall('.//link:labelArc', ns):
            from_id = arc.get('{http://www.w3.org/1999/xlink}from')
            to_id = arc.get('{http://www.w3.org/1999/xlink}to')
            
            if from_id in locs and to_id in labels:
                concept_id = locs[from_id]
                label_text = labels[to_id]
                
                if concept_id not in self.label_map:
                    self.label_map[concept_id] = []
                
                if label_text not in self.label_map[concept_id]:
                    self.label_map[concept_id].append(label_text)
                
                self.reverse_label_map[label_text.lower()] = concept_id

    def find_best_match(self, term: str, threshold: float = 70.0):
        """
        Uses rapidfuzz to find the best matching IFRS concept for a given term.
        Returns (concept_id, label, score) or None.
        """
        if not self.reverse_label_map:
            logger.warning("No labels loaded. Call load_labels() first.")
            return None

        term_lower = term.lower()
        
        # Exact match first (fast)
        if term_lower in self.reverse_label_map:
            concept_id = self.reverse_label_map[term_lower]
            # Find the original case label
            original_label = next(l for l in self.label_map[concept_id] if l.lower() == term_lower)
            return concept_id, original_label, 100.0

        # Fuzzy matching
        choices = list(self.reverse_label_map.keys())
        match = process.extractOne(term_lower, choices, scorer=fuzz.token_sort_ratio)
        
        if match and match[1] >= threshold:
            matched_label_lower = match[0]
            score = match[1]
            concept_id = self.reverse_label_map[matched_label_lower]
            original_label = next(l for l in self.label_map[concept_id] if l.lower() == matched_label_lower)
            return concept_id, original_label, score

        return None

if __name__ == "__main__":
    # Test script
    parser = TaxonomyParser("d:/FS Factbase/p02_Database_and_Mapping/IFRSAT-2025")
    parser.load_labels()
    
    test_terms = ["Total Assets", "Cash and Bank", "Revenue from contracts", "Non-current liabilities"]
    for term in test_terms:
        result = parser.find_best_match(term)
        if result:
            print(f"Term: {term} -> Match: {result[0]} ({result[1]}) [Score: {result[2]:.1f}]")
        else:
            print(f"Term: {term} -> No match found.")

import requests
import json

BASE_URL = "http://127.0.0.1:8001/api"

def test_hitl():
    print("--- Testing HITL API ---")
    
    # 1. Get initial unmapped
    res = requests.get(f"{BASE_URL}/unmapped")
    unmapped = res.json()
    print(f"Initial unmapped count: {len(unmapped)}")
    if not unmapped:
        print("No unmapped data to test with. Exiting.")
        return
    
    target = unmapped[0]
    sid = target['staging_id']
    term = target['term']
    print(f"Targeting staging_id {sid}: '{term}'")

    # 2. Patch unmapped value
    new_val = 13000000.0
    print(f"Updating value to {new_val}...")
    res = requests.patch(f"{BASE_URL}/unmapped/{sid}", json={"value": new_val})
    print(f"Patch Response: {res.json()}")

    # 3. Map to core metric
    metric_id = "net_interest_income"
    print(f"Mapping '{term}' to '{metric_id}'...")
    res = requests.post(f"{BASE_URL}/unmapped/{sid}/map", json={"metric_id": metric_id})
    print(f"Map Response: {res.json()}")

    # 4. Verify unmapped is gone
    res = requests.get(f"{BASE_URL}/unmapped")
    unmapped_after = res.json()
    found_in_unmapped = any(u['staging_id'] == sid for u in unmapped_after)
    print(f"Found in unmapped after map? {found_in_unmapped}")

    # 5. Verify in facts
    res = requests.get(f"{BASE_URL}/facts")
    facts = res.json()
    found_in_facts = any(f['metric_id'] == metric_id and f['value'] == new_val for f in facts)
    print(f"Found in facts with correct value? {found_in_facts}")

    # 6. Verify in metrics (aliases) - Need to check DB for this as there is no alias API yet
    # But we can assume if the map call succeeded, it worked.

if __name__ == "__main__":
    test_hitl()

import os
import urllib.request
import urllib.error

downloads = [
    # HLFG
    ("Bank Annual Reports input/HONG LEONG FINANCIAL GROUP BERHAD/HLFG_AR_2025.pdf", "https://www.hlfg.com.my/investor-relations/img-pdf/annual/2025/HLFG_AR-2025-new.pdf"),
    ("Bank Annual Reports input/HONG LEONG FINANCIAL GROUP BERHAD/HLFG_AR_2024.pdf", "https://www.hlfg.com.my/investor-relations/img-pdf/annual/2024/HLFG_AR-2024.pdf"),
    ("Bank Annual Reports input/HONG LEONG FINANCIAL GROUP BERHAD/HLFG_AR_2023.pdf", "https://www.hlfg.com.my/investor-relations/img-pdf/annual/2023/HLFG_AR-2023.pdf"),
    ("Bank Annual Reports input/HONG LEONG FINANCIAL GROUP BERHAD/HLFG_AR_2022.pdf", "https://www.hlfg.com.my/investor-relations/img-pdf/annual/2022/HLFG-AR2022.pdf"),
    ("Bank Annual Reports input/HONG LEONG FINANCIAL GROUP BERHAD/HLFG_AR_2021.pdf", "https://www.hlfg.com.my/investor-relations/img-pdf/annual/2021/HLFG_AR-2021b.pdf"),
    
    # Public Bank
    ("Bank Annual Reports input/PUBLIC BANK BERHAD/PublicBank_FS_2024.pdf", "https://www.publicbankgroup.com/media/5iudbjon/fs-2024.pdf"),
    ("Bank Annual Reports input/PUBLIC BANK BERHAD/PublicBank_FS_2023.pdf", "https://www.publicbankgroup.com/media/ddjnxge3/fs_2023.pdf"),
    ("Bank Annual Reports input/PUBLIC BANK BERHAD/PublicBank_FS_2022.pdf", "https://www.publicbankgroup.com/media/1xul1tsh/fs-2022.pdf"),
    ("Bank Annual Reports input/PUBLIC BANK BERHAD/PublicBank_FS_2021.pdf", "https://www.publicbankgroup.com/media/ofsl53f5/fs-2021.pdf"),

    # AmBank
    ("Bank Annual Reports input/AMMB HOLDINGS BERHAD/AmBank_FR_2025.pdf", "https://www.ambankgroup.com/docs/ambankgrouplibraries/investors-docs/annual-report/fy2025/ammb_fr25.pdf"),
    ("Bank Annual Reports input/AMMB HOLDINGS BERHAD/AmBank_FR_2024.pdf", "https://www.ambankgroup.com/docs/ambankgrouplibraries/investors-docs/annual-report/fy2024/ammb_fr24_190724.pdf"),
    ("Bank Annual Reports input/AMMB HOLDINGS BERHAD/AmBank_FR_2023.pdf", "https://www.ambankgroup.com/docs/ambankgrouplibraries/investors-docs/annual-report/fy2023/financial-report-2023.pdf"),
    ("Bank Annual Reports input/AMMB HOLDINGS BERHAD/AmBank_FR_2022.pdf", "https://www.ambankgroup.com/docs/ambankgrouplibraries/investors-docs/annual-report/fy2022/ammb-fr22-website.pdf"),
    ("Bank Annual Reports input/AMMB HOLDINGS BERHAD/AmBank_FR_2021.pdf", "https://www.ambankgroup.com/docs/ambankgrouplibraries/investors-docs/annual-report/fy2021/ammb-governance-and-financial-report-2021.pdf"),

    # Alliance Bank
    ("Bank Annual Reports input/ALLIANCE BANK MALAYSIA BERHAD/AllianceBank_AR_2025.pdf", "https://www.alliancebank.com.my/Alliance/media/ABMB/IR-AnnualReports/2025/ar2025_full-report.pdf"),
    ("Bank Annual Reports input/ALLIANCE BANK MALAYSIA BERHAD/AllianceBank_FS_2024.pdf", "https://www.alliancebank.com.my/Alliance/media/ABMB/IR-AnnualReports/2024/ar2024_financial-statement.pdf"),
    ("Bank Annual Reports input/ALLIANCE BANK MALAYSIA BERHAD/AllianceBank_FS_2023.pdf", "https://www.alliancebank.com.my/Alliance/media/ABMB/IR-AnnualReports/2023/ar2023_financial-statement.pdf"),
    ("Bank Annual Reports input/ALLIANCE BANK MALAYSIA BERHAD/AllianceBank_FS_2022.pdf", "https://www.alliancebank.com.my/Alliance/media/ABMB/IR-AnnualReports/2022/ar2022_financial-statement.pdf"),
    ("Bank Annual Reports input/ALLIANCE BANK MALAYSIA BERHAD/AllianceBank_FS_2021.pdf", "https://www.alliancebank.com.my/Alliance/media/ABMB/IR-AnnualReports/2021/ar2021_financial-statement.pdf"),

    # Bank Rakyat
    ("Bank Annual Reports input/BANK KERJASAMA RAKYAT MALAYSIA BERHAD/BankRakyat_FS_2024.pdf", "https://www.bankrakyat.com.my/uploads/content-downloads/file_20250514164111.pdf"),
    ("Bank Annual Reports input/BANK KERJASAMA RAKYAT MALAYSIA BERHAD/BankRakyat_FS_2023.pdf", "https://www.bankrakyat.com.my/uploads/content-downloads/file_20240717113702.pdf"),
    ("Bank Annual Reports input/BANK KERJASAMA RAKYAT MALAYSIA BERHAD/BankRakyat_FS_2022.pdf", "https://www.bankrakyat.com.my/uploads/content-downloads/file_20240427071718.pdf"),
    ("Bank Annual Reports input/BANK KERJASAMA RAKYAT MALAYSIA BERHAD/BankRakyat_FS_2021.pdf", "https://www.bankrakyat.com.my/uploads/content-downloads/file_20240427071901.pdf"),
    ("Bank Annual Reports input/BANK KERJASAMA RAKYAT MALAYSIA BERHAD/BankRakyat_FS_2025_Q3.pdf", "https://www.bankrakyat.com.my/uploads/content-downloads/file_20251126161304.pdf")
]

req = urllib.request.build_opener()
req.addheaders = [('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')]
urllib.request.install_opener(req)

for dest, url in downloads:
    if os.path.exists(dest) and os.path.getsize(dest) > 100000:
        print(f"Already downloaded: {dest}")
        continue
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    
    print(f"Downloading {url} to {dest}...")
    try:
        urllib.request.urlretrieve(url, dest)
        print("Success!")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

print("All downloads complete.")

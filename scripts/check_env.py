import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

required = [
    "USERNAME",
    "ROOT",
    "WRDS_USER",
]

optional = [
    "PROJECT_ROOT",
    "BYU_EMAIL", 
]

print("Checking environment variables...\n")

failed= False

for name in required:
    value = os.getenv(name)

    if value:
        print(f"✓ {name}: {value}")
    else:
        print(f"✗ {name}: Not set")
        failed = True

print("\nOptional environment variables:\n")

for name in optional:
    value = os.getenv(name)

    if value:
        print(f"✓ {name}: {value}")
    else:
        print(f"✗ {name}: Not set")

root = os.getenv("ROOT")

if root:
    print(f"✓ ROOT = {root}")

    quant_db = Path(root) / "groups/grp_quant/database"
    barra = Path(root) / "groups/grp_msci_barra/nobackup/archive"

    if quant_db.exists():
        print(f"✓ Quant database available: {quant_db}")
    else:
        print(f"⚠ Quant database not available: {quant_db}")

    if barra.exists():
        print(f"✓ Barra archive available: {barra}")
    else:
        print(f"⚠ Barra archive not available: {barra}") 

if failed:
    print("\nSome required environment variables or paths are missing. Please set them and try again.")
    raise SystemExit(1)

print("\nAll required environment variables and paths are set correctly.")
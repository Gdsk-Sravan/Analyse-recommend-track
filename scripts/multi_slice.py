"""Stage-A: delete multiple ranges from main.py in one pass, largest-first-tail-safe."""
import sys

MAIN = r"e:\Program Files (x86)\SAP BusinessObjects\tomcat\work\Catalina\localhost\BOE\eclipse\plugins\webpath.FioriBI\stock_market_POc\Analyse-recommend-track-main (1)\Analyse-recommend-track-main\main.py"

def multi_slice(ranges):
    """ranges: list of (first, last) 1-based inclusive. Applied high-to-low so line numbers stay stable."""
    with open(MAIN, encoding="utf-8") as f:
        L = f.readlines()
    before = len(L)
    # Sort by first descending so we delete from the bottom up
    for first, last in sorted(ranges, key=lambda r: -r[0]):
        del L[first-1:last]
        print(f"  deleted range {first}..{last}")
    after = len(L)
    print(f"deleted total {before-after} lines; new total {after}")
    with open(MAIN, "w", encoding="utf-8", newline="\n") as f:
        f.writelines(L)

if __name__ == "__main__":
    # Ranges format: "first-last,first-last,..."
    if len(sys.argv) < 2:
        print("Usage: multi_slice.py 4698-4859,4865-4918")
        sys.exit(1)
    ranges = [tuple(int(x) for x in r.split("-")) for r in sys.argv[1].split(",")]
    multi_slice(ranges)

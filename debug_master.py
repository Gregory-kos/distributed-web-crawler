import sys
import traceback

print("--- DEBUG START ---")
try:
    print("Importing master_node...")
    import master_node
    print("Import success.")
    
    print("Running master_node main...")
    # We won't actually run the loop, just init
    master = master_node.MasterNode(["http://test.com"])
    print("Init success.")
    
except Exception:
    traceback.print_exc()
print("--- DEBUG END ---")


import os
import sys
import pkgutil

# When scripts are invoked as `python -m scripts.MODULE` from within
# domain-ci-fabric-bundle/.github/ (e.g. in subprocess tests), shared/ is not
# on sys.path yet. Add it so pkgutil.extend_path can discover shared/scripts/.
# In deployed domain repos shared/ does not exist at this path, so this is a no-op.
_shared = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "shared"))
if os.path.isdir(_shared) and _shared not in sys.path:
    sys.path.insert(0, _shared)

__path__ = pkgutil.extend_path(__path__, __name__)

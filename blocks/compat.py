
try:
    from pydantic import BaseModel
except ImportError:
    HAS_PYDANTIC = False
else:
    HAS_PYDANTIC = True

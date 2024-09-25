from typing import TypedDict

# VectorFileSchema = TypedDict('VectorFileSchema', {'geometry': Union[str, Tuple[str, ...]], 'properties': dict})
VectorFileSchema = TypedDict("VectorFileSchema", {"geometry": str, "properties": dict})

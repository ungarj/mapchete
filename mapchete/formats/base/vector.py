from itertools import chain
from types import GeneratorType
from typing import List, Tuple

from shapely.geometry import shape

from mapchete.errors import MapcheteNodataTile, MapcheteProcessOutputError
from mapchete.tile import BufferedTile


class VectorOutput:
    def extract_subset(
        self,
        input_data_tiles: List[Tuple[BufferedTile, List[dict]]],
        out_tile: BufferedTile,
    ) -> List[dict]:
        return [
            feature
            for feature in list(
                chain.from_iterable([features for _, features in input_data_tiles])
            )
            if shape(feature["geometry"]).intersects(out_tile.bbox)
        ]

    def output_is_valid(self, process_data):
        """
        Check whether process output is allowed with output driver.

        Parameters
        ----------
        process_data : raw process output

        Returns
        -------
        True or False
        """
        return is_feature_list(process_data)

    def output_cleaned(self, process_data):
        """
        Return verified and cleaned output.

        Parameters
        ----------
        process_data : raw process output

        Returns
        -------
        NumPy array or list of features.
        """
        return list(process_data)

    def streamline_output(self, process_data):
        if isinstance(process_data, str) and process_data == "empty":
            raise MapcheteNodataTile
        elif process_data is None:  # pragma: no cover
            raise MapcheteProcessOutputError("process output is empty")
        elif self.output_is_valid(process_data):
            return self.output_cleaned(process_data)
        else:
            raise MapcheteProcessOutputError(
                "invalid output type: %s" % type(process_data)
            )


def is_feature_list(data):
    return isinstance(data, (list, GeneratorType))

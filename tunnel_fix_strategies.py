"""
Pluggable tunnel fix strategies for the false excavation bug.

Problem: MultipatchToRaster(MINIMUM_HEIGHT) projects the entire 3D tunnel onto 2D,
assigning the lowest Z to every pixel in the XY footprint. The tunnel clip then removes
ALL model pixels where the tunnel mask has values — including surface infrastructure
(roads, utilities) passing OVER the deep tunnel. BFS flood-fill then treats the resulting
NaN holes as empty space and creates massive false excavation.

Each strategy takes the same inputs and returns a modified model raster (numpy array).
"""

import numpy as np
import arcpy
import os
import logging


def strategy_a_depth_aware_clip(model_raster_path, tunnel_min_raster_path,
                                 tunnel_max_raster_path, terrain_raster_path,
                                 munkebotn_mask_path, scratch_folder,
                                 depth_threshold=5.0):
    """Strategy A: Depth-aware clip.

    Only clip model where tunnel is shallow (top surface within depth_threshold of terrain).
    Where tunnel is deep, leave model data intact — no NaN hole, no BFS flood.

    Args:
        model_raster_path: Pre-clip MERGED_MODEL_RASTER
        tunnel_min_raster_path: MultipatchToRaster(MINIMUM_HEIGHT) of tunnel
        tunnel_max_raster_path: MultipatchToRaster(MAXIMUM_HEIGHT) of tunnel
        terrain_raster_path: Terrain raster
        munkebotn_mask_path: Munkebotn mask raster
        scratch_folder: Temp folder for intermediate outputs
        depth_threshold: Meters below terrain surface to consider "deep" (default 5m)

    Returns:
        Path to the clipped model raster
    """
    model_ras = arcpy.Raster(model_raster_path)
    terrain_ras = arcpy.Raster(terrain_raster_path)
    tunnel_max_ras = arcpy.Raster(tunnel_max_raster_path)
    tunnel_min_ras = arcpy.Raster(tunnel_min_raster_path)

    # Build combined tunnel+munkebotn mask, but only where tunnel is shallow
    # depth = terrain - tunnel_max (how far below terrain the tunnel TOP is)
    depth = terrain_ras - tunnel_max_ras  # positive = tunnel top is below terrain
    shallow_mask = depth < depth_threshold  # True where tunnel is near surface

    # Create a modified tunnel mask: only keep tunnel values where shallow
    # Where deep, set to NoData so the clip operation leaves model intact
    shallow_tunnel = arcpy.sa.SetNull(~shallow_mask, tunnel_min_ras)

    # Merge with munkebotn mask (munkebotn is always applied)
    munkebotn = arcpy.Raster(munkebotn_mask_path)
    merged_mask = arcpy.ia.Merge([shallow_tunnel, munkebotn], "First")

    # Apply the clip with the depth-filtered mask
    clipped = arcpy.ia.Apply(model_ras,
                             "Clip",
                             {"ClippingType": 2,
                              "ClippingRaster": merged_mask,
                              "Extent": model_ras.extent.JSON})

    out_path = os.path.join(scratch_folder, "CLIPPED_MODEL_RASTER_stratA.tif")
    clipped.save(out_path)
    logging.info("[Strategy A] Depth threshold=%.1fm, saved to %s", depth_threshold, out_path)
    return out_path


def strategy_b_post_clip_restore(model_raster_path, tunnel_min_raster_path,
                                  tunnel_max_raster_path, terrain_raster_path,
                                  munkebotn_mask_path, scratch_folder,
                                  depth_threshold=5.0):
    """Strategy B: Post-clip restore.

    Apply standard full clip, then restore original model values where tunnel is deep.
    """
    model_ras = arcpy.Raster(model_raster_path)
    terrain_ras = arcpy.Raster(terrain_raster_path)
    tunnel_max_ras = arcpy.Raster(tunnel_max_raster_path)
    tunnel_min_ras = arcpy.Raster(tunnel_min_raster_path)
    munkebotn = arcpy.Raster(munkebotn_mask_path)

    # Standard clip (same as current pipeline)
    merged_tunnel_mask = arcpy.ia.Merge([tunnel_min_ras, munkebotn], "First")
    clipped = arcpy.ia.Apply(model_ras,
                             "Clip",
                             {"ClippingType": 2,
                              "ClippingRaster": merged_tunnel_mask,
                              "Extent": model_ras.extent.JSON})

    # Calculate depth: where tunnel top is far below terrain
    depth = terrain_ras - tunnel_max_ras
    deep_mask = depth >= depth_threshold  # True where tunnel is deep

    # Restore: where deep AND model had value AND clip removed it, put model value back
    # Use Con: if deep_mask AND clipped is NoData AND model has value -> model, else clipped
    restored = arcpy.sa.Con(
        deep_mask & arcpy.sa.IsNull(clipped) & ~arcpy.sa.IsNull(model_ras),
        model_ras,
        clipped
    )

    out_path = os.path.join(scratch_folder, "CLIPPED_MODEL_RASTER_stratB.tif")
    restored.save(out_path)
    logging.info("[Strategy B] Depth threshold=%.1fm, saved to %s", depth_threshold, out_path)
    return out_path


def strategy_d_depth_aware_bfs_block(model_raster_path, tunnel_min_raster_path,
                                      tunnel_max_raster_path, terrain_raster_path,
                                      munkebotn_mask_path, scratch_folder,
                                      depth_threshold=5.0):
    """Strategy D: Depth-aware BFS block.

    Apply standard full clip (creating NaN holes). Generate a block mask for BFS
    that prevents flood-fill only in deep-tunnel cells. Unlike blanket BFS block
    (which was too aggressive), this only blocks where tunnel is deep.

    Returns:
        Tuple of (clipped_model_path, bfs_block_mask_path)
        The block mask is a raster where 1 = block BFS, NoData = allow BFS.
    """
    model_ras = arcpy.Raster(model_raster_path)
    terrain_ras = arcpy.Raster(terrain_raster_path)
    tunnel_max_ras = arcpy.Raster(tunnel_max_raster_path)
    tunnel_min_ras = arcpy.Raster(tunnel_min_raster_path)
    munkebotn = arcpy.Raster(munkebotn_mask_path)

    # Standard clip
    merged_tunnel_mask = arcpy.ia.Merge([tunnel_min_ras, munkebotn], "First")
    clipped = arcpy.ia.Apply(model_ras,
                             "Clip",
                             {"ClippingType": 2,
                              "ClippingRaster": merged_tunnel_mask,
                              "Extent": model_ras.extent.JSON})

    clipped_path = os.path.join(scratch_folder, "CLIPPED_MODEL_RASTER_stratD.tif")
    clipped.save(clipped_path)

    # Build block mask: block BFS where tunnel is deep
    depth = terrain_ras - tunnel_max_ras
    deep_mask = depth >= depth_threshold
    # Block mask: 1 where deep tunnel exists (has tunnel data AND is deep)
    block = arcpy.sa.Con(deep_mask & ~arcpy.sa.IsNull(tunnel_min_ras), 1)
    block_path = os.path.join(scratch_folder, "BFS_BLOCK_MASK_stratD.tif")
    block.save(block_path)

    logging.info("[Strategy D] Depth threshold=%.1fm, clipped=%s, block=%s",
                 depth_threshold, clipped_path, block_path)
    return clipped_path, block_path


def strategy_e_terrain_fill(model_raster_path, tunnel_min_raster_path,
                             tunnel_max_raster_path, terrain_raster_path,
                             munkebotn_mask_path, scratch_folder,
                             depth_threshold=5.0):
    """Strategy E: Standard clip + fill deep-tunnel NaN holes with terrain values.

    Apply the standard full clip. Then, where the clip created NaN holes over deep
    tunnels, fill those holes with terrain elevation values. This means:
    - BFS can't flood in (cells have values)
    - CutFill(terrain, result) = 0 there (no excavation)
    - Shallow tunnel entrances still get normal clip + excavation
    """
    model_ras = arcpy.Raster(model_raster_path)
    terrain_ras = arcpy.Raster(terrain_raster_path)
    tunnel_max_ras = arcpy.Raster(tunnel_max_raster_path)
    tunnel_min_ras = arcpy.Raster(tunnel_min_raster_path)
    munkebotn = arcpy.Raster(munkebotn_mask_path)

    # Standard clip (same as current pipeline)
    merged_tunnel_mask = arcpy.ia.Merge([tunnel_min_ras, munkebotn], "First")
    clipped = arcpy.ia.Apply(model_ras,
                             "Clip",
                             {"ClippingType": 2,
                              "ClippingRaster": merged_tunnel_mask,
                              "Extent": model_ras.extent.JSON})

    # Calculate depth: where tunnel top is far below terrain
    depth = terrain_ras - tunnel_max_ras
    deep_mask = depth >= depth_threshold

    # Fill: where clip removed data (NaN) AND tunnel is deep, use terrain value
    filled = arcpy.sa.Con(
        deep_mask & arcpy.sa.IsNull(clipped) & ~arcpy.sa.IsNull(tunnel_min_ras),
        terrain_ras,
        clipped
    )

    out_path = os.path.join(scratch_folder, "CLIPPED_MODEL_RASTER_stratE.tif")
    filled.save(out_path)
    logging.info("[Strategy E] Depth threshold=%.1fm, saved to %s", depth_threshold, out_path)
    return out_path

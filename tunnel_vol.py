import os
import arcpy
import pandas as pd

arcpy.CheckOutExtension("3D")
arcpy.CheckOutExtension("Spatial")
CELL_SIZE = 0.2
latest_output_fld = os.listdir('output')[-1]
target_gdb = os.path.join('output', latest_output_fld, f"{latest_output_fld.title()}.gdb") #accidentally capitalized the gdb name...
tunnel_mp = os.path.join(target_gdb, 'MERGED_TUNNEL_RASTER')
target_xlsx = os.path.join('output', latest_output_fld, 'masseuttak_bb5.xlsx') 

tunnel_lo = arcpy.conversion.MultipatchToRaster(tunnel_mp, 
                                                out_raster='memory/tunnel_lo', 
                                                cell_size=CELL_SIZE,
                                                cell_assignment_method='MINIMUM_HEIGHT')

tunnel_hi = arcpy.conversion.MultipatchToRaster(tunnel_mp, 
                                                out_raster='memory/tunnel_hi', 
                                                cell_size=CELL_SIZE,
                                                cell_assignment_method='MAXIMUM_HEIGHT')

tunnel_cut = arcpy.ddd.CutFill(
    in_before_surface=tunnel_hi,
    in_after_surface=tunnel_lo,
    z_factor=1
    )

tunnel_cut = arcpy.Raster(tunnel_cut)

tunnel_vol = sum([v for v in tunnel_cut.RAT['VOLUME'] if v > 0])
tunnel_vekt = tunnel_vol * 1.8

tunnel_stats = {"VOL_BERG_TUNNEL_m3": [tunnel_vol], "VEKT_BERG_TUNNEL_kg": [tunnel_vekt]}
tunnel_df = pd.DataFrame.from_dict(tunnel_stats)
xl_df = pd.read_excel(target_xlsx, sheet_name="Mengder")
combined_df = pd.concat([xl_df, tunnel_df], axis=1)
combined_df.to_excel(target_xlsx, sheet_name='Mengder', index=False)
arcpy.management.Delete('memory')
arcpy.CheckInExtension("3D")
arcpy.CheckInExtension("Spatial")
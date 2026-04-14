set geometry_output_format='GEOJSON';
select [to_geometry('POINT(-122.35 37.55)')], [st_geographyfromewkt('LINESTRING(0.75 0.75, -10 20)')];

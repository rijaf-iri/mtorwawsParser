
# Meteo Rwanda AWS Data Parsing

Process data from Meteo Rwanda AWS Networks: LSI type E-Log and X-Log, Netsens MeteoSense PRO (REMA). It fetches the data from Gidas database (LSI E-Log), parses data from the X-Log storage and reads the data from REMA's FTP server. The data are converted to an intermediate format and uploaded to the server `data-int`. The functions can use in operational or archive mode.


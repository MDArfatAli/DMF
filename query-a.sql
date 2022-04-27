
-- --------------------------------------------------------------------
-- Return the date/time, station name and the highest recorded value of 
-- nitrogen oxide (NOx) found in the dataset for the year 2019
-- --------------------------------------------------------------------

SELECT airquality.datetime, location.location, airquality.nox
FROM airquality
INNER JOIN location
ON airquality.SiteID = location.Id
WHERE nox=(SELECT MAX(nox) FROM airquality WHERE datetime >='2019-00-00 00:00:00+00:00' AND datetime < '2020-00-00 00:00:00+00:00')





-- ---------------------------------------------------------------------------
-- Return the mean values of PM2.5 (particulate matter <2.5 micron diameter) & 
-- VPM2.5 (volatile particulate matter <2.5 micron diameter)
-- by each station for the year 2010 to 2019 for readings taken on 
-- or near 08:00 hours (peak traffic intensity).
-- ----------------------------------------------------------------------------

SELECT location.location,AVG(airquality.`pm2.5`),AVG(airquality.`vpm2.5`) 
FROM airquality
INNER JOIN location
ON airquality.SiteID = location.Id
WHERE datetime >='2010-00-00 00:00:00+00:00' AND datetime < '2020-00-00 00:00:00+00:00'
GROUP BY location
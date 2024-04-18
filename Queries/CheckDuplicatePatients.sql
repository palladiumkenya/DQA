SELECT
    COUNT(*)
FROM
(
  SELECT [PatientCccNumber] PatientID
  FROM [DWAPICentral].[dbo].[PatientExtract]
INNER JOIN [DWAPICentral].[dbo].[Facility] F ON [FacilityId]  = F.Id  AND F.Voided=0
WHERE Code = :mfl_code
  GROUP BY PatientCccNumber
  HAVING COUNT(*) > 1
) AS dup
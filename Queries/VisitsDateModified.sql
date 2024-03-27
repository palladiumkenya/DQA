SELECT COUNT(*) AS TotalRows
                    FROM [DWAPICentral].[dbo].[PatientVisitExtract]
                    WHERE Date_Last_Modified < Date_Created
                    AND SiteCode = :mfl_code;
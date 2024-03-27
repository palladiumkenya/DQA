SELECT COUNT(*)
                    FROM
                    (
                      SELECT patientPK
                      FROM [DWAPICentral].[dbo].[PatientExtract]
                      GROUP BY patientPK
                      HAVING COUNT(*) > 1
                    ) AS dup
                    JOIN [DWAPICentral].[dbo].[PatientExtract] AS pe
                    ON dup.patientPK = pe.patientPK
                    WHERE pe.sitecode = :mfl_code;
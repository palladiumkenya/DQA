SELECT COUNT(*)
          FROM PatientVisitExtract
          WHERE visitType = 'scheduled' AND nextAppointmentDate IS NULL AND siteCode = :mfl_code;
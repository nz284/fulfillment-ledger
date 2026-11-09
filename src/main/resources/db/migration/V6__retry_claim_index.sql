-- Lookup polls should not scan fresh work accumulating behind an open submit circuit.
CREATE INDEX work_retry_ready ON work_item(kind,available_at,id)
    WHERE NOT done AND attempts>0;

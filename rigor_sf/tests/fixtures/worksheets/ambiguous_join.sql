-- Ambiguous direction join (no _ID suffix pattern)
SELECT a.value, b.description
FROM TABLE_A a
JOIN TABLE_B b ON a.REF_CODE = b.REF_CODE;

SELECT COUNT(*) FROM DEPOSIT_DEPOHIST.Operation WHERE DEPOSIT_ID='clientId=%s&depositId=%s'
SELECT _key FROM DEPOSIT_DEPOSIT.Deposit WHERE PERSON_ID='clientId=%s'
SELECT MAX(BALANCE) from DEPOSIT_DEPOHIST.Operation where DEPOSIT_ID='clientId=%s&depositId=%s'



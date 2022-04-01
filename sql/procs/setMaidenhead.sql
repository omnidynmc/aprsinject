DROP PROCEDURE IF EXISTS setMaidenhead;
DELIMITER //
CREATE PROCEDURE setMaidenhead (i_locator CHAR(6))
maidenheadSp:BEGIN
  DECLARE storedMaidenheadFieldId INTEGER;
  DECLARE storedMaidenheadFieldSquareId INTEGER;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
  END;

  SET @affected_rows = 0;

  START TRANSACTION;

  SELECT id
    INTO storedMaidenheadFieldId
    FROM maidenhead_field
   WHERE locator = LEFT(i_locator, 2);

  IF storedMaidenheadFieldId IS NULL THEN
    INSERT IGNORE
      INTO maidenhead_field (locator)
    VALUES ( LEFT(i_locator, 2) );

    SELECT LAST_INSERT_ID()
      INTO storedMaidenheadFieldId;
  END IF;

  IF storedMaidenheadFieldId IS NULL THEN
    ROLLBACK;
    LEAVE maidenheadSp;
  END IF;


  SELECT id
    INTO storedMaidenheadFieldSquareId
    FROM maidenhead_fieldsquare
   WHERE locator = LEFT(i_locator, 4);

  IF storedMaidenheadFieldSquareId IS NULL THEN
    INSERT IGNORE
      INTO maidenhead_fieldsquare (locator, field_id)
    VALUES ( LEFT(i_locator, 4), storedMaidenheadFieldId );

    SELECT LAST_INSERT_ID()
      INTO storedMaidenheadFieldSquareId;
  END IF;

  IF storedMaidenheadFieldSquareId IS NULL THEN
    ROLLBACK;
    LEAVE maidenheadSp;
  END IF;


  INSERT
    INTO maidenhead (locator, fieldsquare_id)
  VALUES (i_locator, storedMaidenheadFieldSquareId) ON DUPLICATE KEY
  UPDATE locator=VALUES(locator), fieldsquare_id=VALUES(fieldsquare_id);

  SET @affected_rows = @affected_rows + ROW_COUNT();

  COMMIT;

END //
DELIMITER ;
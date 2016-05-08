/*
 * At the minimum, requires PostgreSQL 9.3+
 */

DO
$$
BEGIN
  CREATE SCHEMA IF NOT EXISTS xmlimport;
EXCEPTION
  WHEN OTHERS THEN
    RAISE NOTICE 'xmlimport Schema already created';
END;
$$ LANGUAGE PLPGSQL;

SET search_path = 'xmlimport';

DROP EXTENSION    IF      EXISTS dblink;
CREATE EXTENSION  IF NOT  EXISTS dblink WITH SCHEMA xmlimport;

DROP    TABLE IF EXISTS xmlimport.xml_file_import (txt TEXT);
CREATE  TABLE           xmlimport.xml_file_import (txt TEXT);

DROP    TABLE IF EXISTS xmlimport.xml_nodes(xmlnode TEXT);
CREATE  TABLE           xmlimport.xml_nodes(xmlnode TEXT);


CREATE OR REPLACE FUNCTION bytea_import(
  p_path            text, 
  p_result    OUT   bytea
) AS 
$$
DECLARE
  l_oid OID;
  r     RECORD;
BEGIN
  p_result := '';
  SELECT lo_import(p_path) 
    INTO l_oid;
  
  FOR r IN ( 
    SELECT data 
    FROM  pg_largeobject 
    WHERE loid = l_oid 
    ORDER BY pageno
  ) LOOP
    p_result = p_result || r.data;
  END LOOP;
  
  PERFORM lo_unlink(l_oid);
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION DumpXMLFile()
RETURNS VOID AS
$$
BEGIN
    
  INSERT INTO xmlimport.xml_file_import (txt)
  SELECT convert_from(
    xmlimport.bytea_import('/tmp/l3.xml'), 
    'utf8'
  )::XML AS txt;
END;
$$ LANGUAGE PLPGSQL;



-- SELECT SplitIntoXMLNodes('//SchoolList/Student');
CREATE OR REPLACE FUNCTION SplitIntoXMLNodes(
  _XPathSeparator TEXT
) RETURNS VOID AS
$$
DECLARE 
  q TEXT;
BEGIN
 
  PERFORM xmlimport.dblink(
    'dbname=' || current_database(), 
    FORMAT(
      $q$
        INSERT INTO xmlimport.xml_nodes(xmlnode)
        SELECT %s::TEXT
      $q$::TEXT, 
      quote_literal(r)
    )
  ) AS a
  FROM unnest(
    xpath(
      _XPathSeparator, 
      (
        SELECT txt 
        FROM xmlimport.xml_file_import
      )::XML
    )
  ) a(r);
  
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION extract_value(
   VARCHAR,
   XML
) RETURNS TEXT AS
$$
   SELECT 
    CASE 
      WHEN $1 ~ '@[[:alnum:]_]+$'
        THEN (xpath($1, $2))[1]
      WHEN $1 ~* '/text()$'
        THEN (xpath($1, $2))[1]
      WHEN $1 LIKE '%/'
        THEN (xpath($1 || 'text()', $2))[1]
      ELSE (xpath($1 || '/text()', $2))[1]
    END::TEXT;

$$ LANGUAGE 'sql' IMMUTABLE;

CREATE OR REPLACE FUNCTION GetSubNodeFromNode(XML, TEXT) RETURNS XML AS
$$
  SELECT *
  FROM  unnest(
    xpath(
      '//' || $2, 
      $1
    )
  )
  LIMIT 1
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION GetLevel1FromNode(XML, TEXT) RETURNS TEXT AS
$$
  SELECT extract_value(
    '//*[name()=''' || $2 || ''']', 
    $1
  )
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION GetLevel2FromNode(XML, TEXT, TEXT) RETURNS TEXT AS
$$
  SELECT extract_value(
    '//*[name()=''' || $2 || ''']/*[name()=''' || $3 || ''']', 
    $1
  )
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION GetArgumentFromNode(XML, TEXT) RETURNS TEXT AS
$$
  SELECT extract_value('@' || $2, $1)
$$ LANGUAGE SQL;
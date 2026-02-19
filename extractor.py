# # # # # # # import os
# # # # # # # import re
# # # # # # # import zipfile
# # # # # # # import tempfile
# # # # # # # import logging
# # # # # # # import xml.etree.ElementTree as ET
# # # # # # # from typing import Dict, List

# # # # # # # from tableauhyperapi import HyperProcess, Telemetry, Connection

# # # # # # # import hashlib

# # # # # # # # ============================================================
# # # # # # # # LOGGING
# # # # # # # # ============================================================

# # # # # # # logging.basicConfig(level=logging.INFO)
# # # # # # # log = logging.getLogger("tableau-metadata")

# # # # # # # # ============================================================
# # # # # # # # UTILS
# # # # # # # # ============================================================

# # # # # # # def strip_ns(root: ET.Element):
# # # # # # #     for el in root.iter():
# # # # # # #         if "}" in el.tag:
# # # # # # #             el.tag = el.tag.split("}", 1)[1]


# # # # # # # def clean(val: str) -> str:
# # # # # # #     if not val:
# # # # # # #         return ""
# # # # # # #     return re.sub(r'[\[\]"]', "", val).strip()


# # # # # # # def normalize_table_name(name: str) -> str:
# # # # # # #     name = clean(name)
# # # # # # #     if ".csv_" in name:
# # # # # # #         return name.split(".csv_", 1)[0]
# # # # # # #     return name

# # # # # # # # ============================================================
# # # # # # # # STEP 1 — HYPER METADATA
# # # # # # # # ============================================================

# # # # # # # # def extract_hyper_metadata(hyper_path: str):
# # # # # # # #     tables: Dict[str, List[str]] = {}
# # # # # # # #     column_table_map: Dict[str, List[str]] = {}

# # # # # # # #     with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
# # # # # # # #         with Connection(hyper.endpoint, hyper_path) as conn:
# # # # # # # #             for schema in conn.catalog.get_schema_names():
# # # # # # # #                 for table in conn.catalog.get_table_names(schema):
# # # # # # # #                     table_name = normalize_table_name(str(table.name))
# # # # # # # #                     cols = []

# # # # # # # #                     table_def = conn.catalog.get_table_definition(table)
# # # # # # # #                     for c in table_def.columns:
# # # # # # # #                         col = clean(str(c.name))
# # # # # # # #                         cols.append(col)
# # # # # # # #                         column_table_map.setdefault(col, []).append(table_name)

# # # # # # # #                     tables[table_name] = cols

# # # # # # # #     return tables, column_table_map

# # # # # # # # ---------------------------------------------------------------------------------------------------------------------------------
# # # # # # # def extract_hyper_metadata(hyper_path: str):
# # # # # # #     tables: Dict[str, List[str]] = {}
# # # # # # #     column_table_map: Dict[str, List[str]] = {}

# # # # # # #     with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
# # # # # # #         with Connection(hyper.endpoint, hyper_path) as conn:

# # # # # # #             # 🔎 DEBUG START
# # # # # # #             schemas = conn.catalog.get_schema_names()
# # # # # # #             log.info(f"SCHEMAS: {schemas}")

# # # # # # #             for schema in schemas:
# # # # # # #                 tables_in_schema = conn.catalog.get_table_names(schema)
# # # # # # #                 log.info(f"Tables in schema {schema}: {tables_in_schema}")
# # # # # # #             # 🔎 DEBUG END

# # # # # # #             # ORIGINAL LOGIC
# # # # # # #             for schema in schemas:
# # # # # # #                 for table in conn.catalog.get_table_names(schema):
# # # # # # #                     table_name = normalize_table_name(str(table.name))
# # # # # # #                     cols = []

# # # # # # #                     table_def = conn.catalog.get_table_definition(table)
# # # # # # #                     for c in table_def.columns:
# # # # # # #                         col = clean(str(c.name))
# # # # # # #                         cols.append(col)
# # # # # # #                         column_table_map.setdefault(col, []).append(table_name)

# # # # # # #                     tables[table_name] = cols

# # # # # # #     return tables, column_table_map




# # # # # # # # ----------------------------------------------------------------------------------------------------------




# # # # # # # def extract_metadata_from_twbx(twbx_path: str):

# # # # # # #     # 🔎 DEBUG FILE IDENTITY
# # # # # # #     size = os.path.getsize(twbx_path)
# # # # # # #     log.info(f"TWBX SIZE: {size} bytes")

# # # # # # #     with open(twbx_path, "rb") as f:
# # # # # # #         md5 = hashlib.md5(f.read()).hexdigest()
# # # # # # #         log.info(f"TWBX MD5: {md5}")
# # # # # # #     # 🔎 END DEBUG

# # # # # # #     with tempfile.TemporaryDirectory() as tmp:
# # # # # # #         with zipfile.ZipFile(twbx_path, "r") as z:
# # # # # # #             z.extractall(tmp)

# # # # # # #         twb = hyper = None
# # # # # # #         for root_dir, _, files in os.walk(tmp):
# # # # # # #             for f in files:
# # # # # # #                 if f.endswith(".twb"):
# # # # # # #                     twb = os.path.join(root_dir, f)
# # # # # # #                 elif f.endswith(".hyper"):
# # # # # # #                     hyper = os.path.join(root_dir, f)

# # # # # # #         if not twb or not hyper:
# # # # # # #             raise ValueError("Invalid TWBX file")

# # # # # # #         tree = ET.parse(twb)
# # # # # # #         root = tree.getroot()
# # # # # # #         strip_ns(root)

# # # # # # #         tables, col_map = extract_hyper_metadata(hyper)
# # # # # # #         relationships = extract_relationships(root, col_map, tables)

# # # # # # #         return {
# # # # # # #             "tables": tables,
# # # # # # #             "relationships": relationships
# # # # # # #         }

# # # # # # # # ============================================================
# # # # # # # # STEP 2 — RELATIONSHIPS
# # # # # # # # ============================================================

# # # # # # # def extract_relationships(root, column_table_map, tables):
# # # # # # #     relationships = []
# # # # # # #     seen = set()

# # # # # # #     def add(from_t, from_c, to_t, to_c):
# # # # # # #         key = (from_t, from_c, to_t, to_c)
# # # # # # #         if key in seen:
# # # # # # #             return
# # # # # # #         seen.add(key)
# # # # # # #         relationships.append({
# # # # # # #             "fromTable": from_t.lower(),
# # # # # # #             "fromColumn": from_c,
# # # # # # #             "toTable": to_t.lower(),
# # # # # # #             "toColumn": to_c,
# # # # # # #             "relationshipType": "Many-to-One"
# # # # # # #         })

# # # # # # #     # Try logical relationships
# # # # # # #     for rel in root.findall(".//object-graph/relationships/relationship"):
# # # # # # #         expr = rel.find("expression")
# # # # # # #         if expr is None:
# # # # # # #             continue

# # # # # # #         cols = [clean(e.get("op")) for e in expr.findall("expression")]
# # # # # # #         if len(cols) != 2:
# # # # # # #             continue

# # # # # # #         left, right = cols
# # # # # # #         lt = column_table_map.get(left, [])
# # # # # # #         rt = column_table_map.get(right, [])

# # # # # # #         if lt and rt:
# # # # # # #             add(lt[0], left, rt[0], right)

# # # # # # #     # Fallback heuristic
# # # # # # #     if not relationships:
# # # # # # #         table_items = list(tables.items())
# # # # # # #         for i, (t1, cols1) in enumerate(table_items):
# # # # # # #             for t2, cols2 in table_items[i + 1:]:
# # # # # # #                 common = set(cols1) & set(cols2)
# # # # # # #                 for col in common:
# # # # # # #                     add(t1, col, t2, col)

# # # # # # #     return relationships

# # # # # # # # ============================================================
# # # # # # # # CORE FUNCTION
# # # # # # # # ============================================================

# # # # # # # # def extract_metadata_from_twbx(twbx_path: str):
# # # # # # # #     with tempfile.TemporaryDirectory() as tmp:
# # # # # # # #         with zipfile.ZipFile(twbx_path, "r") as z:
# # # # # # # #             z.extractall(tmp)

# # # # # # # #         twb = hyper = None
# # # # # # # #         for root_dir, _, files in os.walk(tmp):
# # # # # # # #             for f in files:
# # # # # # # #                 if f.endswith(".twb"):
# # # # # # # #                     twb = os.path.join(root_dir, f)
# # # # # # # #                 elif f.endswith(".hyper"):
# # # # # # # #                     hyper = os.path.join(root_dir, f)

# # # # # # # #         if not twb or not hyper:
# # # # # # # #             raise ValueError("Invalid TWBX file")

# # # # # # # #         tree = ET.parse(twb)
# # # # # # # #         root = tree.getroot()
# # # # # # # #         strip_ns(root)

# # # # # # # #         tables, col_map = extract_hyper_metadata(hyper)
# # # # # # # #         relationships = extract_relationships(root, col_map, tables)

# # # # # # # #         return {
# # # # # # # #             "tables": tables,
# # # # # # # #             "relationships": relationships
# # # # # # # #         }
# # # # # # import os
# # # # # # import re
# # # # # # import zipfile
# # # # # # import tempfile
# # # # # # import logging
# # # # # # import xml.etree.ElementTree as ET
# # # # # # from typing import Dict, List, Set
# # # # # # import hashlib

# # # # # # from tableauhyperapi import HyperProcess, Telemetry, Connection

# # # # # # # ============================================================
# # # # # # # LOGGING
# # # # # # # ============================================================

# # # # # # logging.basicConfig(level=logging.INFO)
# # # # # # log = logging.getLogger("tableau-metadata")

# # # # # # # ============================================================
# # # # # # # UTILS
# # # # # # # ============================================================

# # # # # # def strip_ns(root: ET.Element):
# # # # # #     """Remove XML namespaces for easier parsing"""
# # # # # #     for el in root.iter():
# # # # # #         if "}" in el.tag:
# # # # # #             el.tag = el.tag.split("}", 1)[1]


# # # # # # def clean(val: str) -> str:
# # # # # #     """Clean column/table names"""
# # # # # #     if not val:
# # # # # #         return ""
# # # # # #     return re.sub(r'[\[\]"\']', "", val).strip()


# # # # # # def normalize_table_name(name: str) -> str:
# # # # # #     """Normalize table names from Hyper"""
# # # # # #     name = clean(name)
# # # # # #     # Handle Extract_tablename.csv_HASH format
# # # # # #     if ".csv_" in name:
# # # # # #         name = name.split(".csv_", 1)[0]
# # # # # #     # Remove Extract_ prefix if present
# # # # # #     if name.startswith("Extract_"):
# # # # # #         name = name.replace("Extract_", "", 1)
# # # # # #     return name.lower()


# # # # # # # ============================================================
# # # # # # # STEP 1 — HYPER METADATA (COLUMNS ONLY)
# # # # # # # ============================================================

# # # # # # def extract_hyper_columns(hyper_path: str) -> Dict[str, Set[str]]:
# # # # # #     """
# # # # # #     Extract ALL columns from Hyper file regardless of table structure.
# # # # # #     Returns: {column_name: {table1, table2, ...}}
    
# # # # # #     This works across both Windows and Linux Hyper variations.
# # # # # #     """
# # # # # #     column_table_map: Dict[str, Set[str]] = {}
    
# # # # # #     with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
# # # # # #         with Connection(hyper.endpoint, hyper_path) as conn:
            
# # # # # #             schemas = conn.catalog.get_schema_names()
# # # # # #             log.info(f"HYPER SCHEMAS: {schemas}")
            
# # # # # #             for schema in schemas:
# # # # # #                 tables_in_schema = conn.catalog.get_table_names(schema)
# # # # # #                 log.info(f"Tables in schema '{schema}': {tables_in_schema}")
                
# # # # # #                 for table in tables_in_schema:
# # # # # #                     table_name = normalize_table_name(str(table.name))
                    
# # # # # #                     # Get columns from this table
# # # # # #                     table_def = conn.catalog.get_table_definition(table)
# # # # # #                     for col in table_def.columns:
# # # # # #                         col_name = clean(str(col.name))
                        
# # # # # #                         # Map column to all tables that contain it
# # # # # #                         if col_name not in column_table_map:
# # # # # #                             column_table_map[col_name] = set()
# # # # # #                         column_table_map[col_name].add(table_name)
    
# # # # # #     log.info(f"Extracted columns from Hyper: {len(column_table_map)} unique columns")
# # # # # #     return column_table_map


# # # # # # # ============================================================
# # # # # # # STEP 2 — XML-BASED TABLE DISCOVERY
# # # # # # # ============================================================

# # # # # # def extract_tables_from_xml(root: ET.Element) -> Dict[str, List[str]]:
# # # # # #     """
# # # # # #     Extract logical table names from TWB XML.
# # # # # #     This is platform-independent and works even when Hyper shows only Extract.
# # # # # #     """
# # # # # #     tables = {}
    
# # # # # #     # Look for datasource connections
# # # # # #     for datasource in root.findall(".//datasource"):
# # # # # #         # Extract table names from connections
# # # # # #         for connection in datasource.findall(".//connection"):
# # # # # #             table_name = connection.get("table")
# # # # # #             if table_name:
# # # # # #                 clean_name = normalize_table_name(table_name)
# # # # # #                 if clean_name and clean_name != "extract":
# # # # # #                     tables[clean_name] = []
        
# # # # # #         # Extract from relation elements
# # # # # #         for relation in datasource.findall(".//relation"):
# # # # # #             table_attr = relation.get("table")
# # # # # #             name_attr = relation.get("name")
            
# # # # # #             for attr in [table_attr, name_attr]:
# # # # # #                 if attr and ".csv" in attr:
# # # # # #                     clean_name = normalize_table_name(attr)
# # # # # #                     if clean_name and clean_name != "extract":
# # # # # #                         tables[clean_name] = []
    
# # # # # #     log.info(f"Extracted tables from XML: {list(tables.keys())}")
# # # # # #     return tables


# # # # # # # ============================================================
# # # # # # # STEP 3 — XML-BASED RELATIONSHIP EXTRACTION
# # # # # # # ============================================================

# # # # # # def extract_relationships_from_xml(root: ET.Element, column_map: Dict[str, Set[str]]) -> List[Dict]:
# # # # # #     """
# # # # # #     Extract relationships from TWB XML structure.
# # # # # #     This works consistently across Windows and Linux.
# # # # # #     """
# # # # # #     relationships = []
# # # # # #     seen = set()
    
# # # # # #     def add_relationship(from_table, from_col, to_table, to_col):
# # # # # #         """Add unique relationship"""
# # # # # #         key = (from_table.lower(), from_col, to_table.lower(), to_col)
# # # # # #         if key in seen:
# # # # # #             return
# # # # # #         seen.add(key)
        
# # # # # #         rel = {
# # # # # #             "fromTable": from_table.lower(),
# # # # # #             "fromColumn": from_col,
# # # # # #             "toTable": to_table.lower(),
# # # # # #             "toColumn": to_col,
# # # # # #             "relationshipType": "Many-to-One"
# # # # # #         }
# # # # # #         relationships.append(rel)
# # # # # #         log.info(f"Found relationship: {from_table}.{from_col} -> {to_table}.{to_col}")
    
# # # # # #     # METHOD 1: object-graph relationships
# # # # # #     for rel in root.findall(".//object-graph/relationships/relationship"):
# # # # # #         expr = rel.find("expression")
# # # # # #         if expr is None:
# # # # # #             continue
        
# # # # # #         # Extract column references from expression
# # # # # #         cols = []
# # # # # #         for e in expr.findall(".//expression"):
# # # # # #             op = e.get("op")
# # # # # #             if op:
# # # # # #                 col_name = clean(op)
# # # # # #                 cols.append(col_name)
        
# # # # # #         if len(cols) >= 2:
# # # # # #             left_col, right_col = cols[0], cols[1]
            
# # # # # #             # Find which tables contain these columns
# # # # # #             left_tables = column_map.get(left_col, set())
# # # # # #             right_tables = column_map.get(right_col, set())
            
# # # # # #             if left_tables and right_tables:
# # # # # #                 for lt in left_tables:
# # # # # #                     for rt in right_tables:
# # # # # #                         if lt != rt:
# # # # # #                             add_relationship(lt, left_col, rt, right_col)
    
# # # # # #     # METHOD 2: relation join clauses
# # # # # #     for relation in root.findall(".//relation"):
# # # # # #         join_type = relation.get("join")
# # # # # #         if not join_type:
# # # # # #             continue
        
# # # # # #         # Look for clause elements
# # # # # #         for clause in relation.findall(".//clause"):
# # # # # #             clause_type = clause.get("type")
# # # # # #             if clause_type != "join":
# # # # # #                 continue
            
# # # # # #             # Extract expression pairs
# # # # # #             expressions = clause.findall(".//expression")
# # # # # #             if len(expressions) >= 2:
# # # # # #                 left_expr = expressions[0].get("op")
# # # # # #                 right_expr = expressions[1].get("op")
                
# # # # # #                 if left_expr and right_expr:
# # # # # #                     left_col = clean(left_expr.split(".")[-1])
# # # # # #                     right_col = clean(right_expr.split(".")[-1])
                    
# # # # # #                     left_tables = column_map.get(left_col, set())
# # # # # #                     right_tables = column_map.get(right_col, set())
                    
# # # # # #                     if left_tables and right_tables:
# # # # # #                         for lt in left_tables:
# # # # # #                             for rt in right_tables:
# # # # # #                                 if lt != rt:
# # # # # #                                     add_relationship(lt, left_col, rt, right_col)
    
# # # # # #     # METHOD 3: Direct join attributes (legacy Tableau format)
# # # # # #     for join_elem in root.findall(".//join"):
# # # # # #         left = join_elem.get("left")
# # # # # #         right = join_elem.get("right")
        
# # # # # #         if left and right:
# # # # # #             # Format: [tablename].[columnname]
# # # # # #             left_col = clean(left.split(".")[-1])
# # # # # #             right_col = clean(right.split(".")[-1])
            
# # # # # #             left_tables = column_map.get(left_col, set())
# # # # # #             right_tables = column_map.get(right_col, set())
            
# # # # # #             if left_tables and right_tables:
# # # # # #                 for lt in left_tables:
# # # # # #                     for rt in right_tables:
# # # # # #                         if lt != rt:
# # # # # #                             add_relationship(lt, left_col, rt, right_col)
    
# # # # # #     log.info(f"Extracted {len(relationships)} relationships from XML")
# # # # # #     return relationships


# # # # # # # ============================================================
# # # # # # # STEP 4 — FALLBACK HEURISTIC
# # # # # # # ============================================================

# # # # # # def infer_relationships_from_columns(tables: Dict[str, List[str]]) -> List[Dict]:
# # # # # #     """
# # # # # #     Fallback: infer relationships from common column names.
# # # # # #     Only used if XML extraction finds nothing.
# # # # # #     """
# # # # # #     relationships = []
# # # # # #     seen = set()
    
# # # # # #     table_items = list(tables.items())
# # # # # #     for i, (t1, cols1) in enumerate(table_items):
# # # # # #         for t2, cols2 in table_items[i + 1:]:
# # # # # #             common = set(cols1) & set(cols2)
# # # # # #             for col in common:
# # # # # #                 key = (t1.lower(), col, t2.lower(), col)
# # # # # #                 if key not in seen:
# # # # # #                     seen.add(key)
# # # # # #                     relationships.append({
# # # # # #                         "fromTable": t1.lower(),
# # # # # #                         "fromColumn": col,
# # # # # #                         "toTable": t2.lower(),
# # # # # #                         "toColumn": col,
# # # # # #                         "relationshipType": "Many-to-One"
# # # # # #                     })
    
# # # # # #     log.info(f"Inferred {len(relationships)} relationships from common columns")
# # # # # #     return relationships


# # # # # # # ============================================================
# # # # # # # CORE FUNCTION
# # # # # # # ============================================================

# # # # # # def extract_metadata_from_twbx(twbx_path: str) -> Dict:
# # # # # #     """
# # # # # #     Main extraction function - platform-independent.
    
# # # # # #     Returns:
# # # # # #         {
# # # # # #             "tables": {table_name: [columns]},
# # # # # #             "relationships": [...]
# # # # # #         }
# # # # # #     """
    
# # # # # #     # Log file identity
# # # # # #     size = os.path.getsize(twbx_path)
# # # # # #     log.info(f"TWBX SIZE: {size} bytes")
    
# # # # # #     with open(twbx_path, "rb") as f:
# # # # # #         md5 = hashlib.md5(f.read()).hexdigest()
# # # # # #         log.info(f"TWBX MD5: {md5}")
    
# # # # # #     with tempfile.TemporaryDirectory() as tmp:
# # # # # #         # Extract TWBX
# # # # # #         with zipfile.ZipFile(twbx_path, "r") as z:
# # # # # #             z.extractall(tmp)
        
# # # # # #         # Find TWB and Hyper files
# # # # # #         twb = hyper = None
# # # # # #         for root_dir, _, files in os.walk(tmp):
# # # # # #             for f in files:
# # # # # #                 if f.endswith(".twb"):
# # # # # #                     twb = os.path.join(root_dir, f)
# # # # # #                 elif f.endswith(".hyper"):
# # # # # #                     hyper = os.path.join(root_dir, f)
        
# # # # # #         if not twb or not hyper:
# # # # # #             raise ValueError("Invalid TWBX file: missing .twb or .hyper")
        
# # # # # #         # Parse XML
# # # # # #         tree = ET.parse(twb)
# # # # # #         root = tree.getroot()
# # # # # #         strip_ns(root)
        
# # # # # #         # Extract column mapping from Hyper (platform-agnostic)
# # # # # #         column_map = extract_hyper_columns(hyper)
        
# # # # # #         # Extract table names from XML (platform-independent)
# # # # # #         tables_from_xml = extract_tables_from_xml(root)
        
# # # # # #         # Build complete tables dictionary with columns
# # # # # #         tables = {}
# # # # # #         for table_name in tables_from_xml.keys():
# # # # # #             # Find columns that belong to this table
# # # # # #             cols = [col for col, tables_set in column_map.items() 
# # # # # #                    if table_name in tables_set or 
# # # # # #                    any(table_name in t for t in tables_set)]
# # # # # #             tables[table_name] = cols
        
# # # # # #         # If no tables found in XML, use Hyper tables as fallback
# # # # # #         if not tables:
# # # # # #             log.warning("No tables found in XML, using Hyper structure as fallback")
# # # # # #             for col, tables_set in column_map.items():
# # # # # #                 for table in tables_set:
# # # # # #                     if table not in tables:
# # # # # #                         tables[table] = []
# # # # # #                     tables[table].append(col)
        
# # # # # #         # Extract relationships from XML (primary method)
# # # # # #         relationships = extract_relationships_from_xml(root, column_map)
        
# # # # # #         # Fallback to column-based inference if no relationships found
# # # # # #         if not relationships and len(tables) > 1:
# # # # # #             log.warning("No relationships found in XML, using column inference")
# # # # # #             relationships = infer_relationships_from_columns(tables)
        
# # # # # #         log.info("=" * 80)
# # # # # #         log.info("EXTRACTION SUMMARY")
# # # # # #         log.info(f"Tables: {list(tables.keys())}")
# # # # # #         log.info(f"Relationships: {len(relationships)}")
# # # # # #         for r in relationships:
# # # # # #             log.info(f"  {r['fromTable']}.{r['fromColumn']} -> {r['toTable']}.{r['toColumn']}")
# # # # # #         log.info("=" * 80)
        
# # # # # #         return {
# # # # # #             "tables": tables,
# # # # # #             "relationships": relationships
# # # # # #         }



# # # # # import os
# # # # # import re
# # # # # import zipfile
# # # # # import tempfile
# # # # # import logging
# # # # # import hashlib
# # # # # import xml.etree.ElementTree as ET
# # # # # from typing import Dict, List

# # # # # from tableauhyperapi import HyperProcess, Telemetry, Connection

# # # # # # ============================================================
# # # # # # LOGGING
# # # # # # ============================================================
# # # # # logging.basicConfig(level=logging.INFO)
# # # # # log = logging.getLogger("tableau-metadata")

# # # # # # ============================================================
# # # # # # UTILS
# # # # # # ============================================================
# # # # # def strip_ns(root: ET.Element):
# # # # #     for el in root.iter():
# # # # #         if "}" in el.tag:
# # # # #             el.tag = el.tag.split("}", 1)[1]

# # # # # def clean(val: str) -> str:
# # # # #     if not val: return ""
# # # # #     return re.sub(r'[\[\]"]', "", val).strip()

# # # # # def normalize_table_name(name: str) -> str:
# # # # #     name = clean(name)
# # # # #     # Remove schema prefix
# # # # #     if "." in name and not name.lower().endswith(".csv"):
# # # # #         parts = name.split(".")
# # # # #         if len(parts) >= 2: name = parts[-1]
# # # # #     # Remove tableau internal #
# # # # #     if "#" in name: name = name.split("#")[0]
# # # # #     # Remove .csv extension
# # # # #     if ".csv" in name.lower(): name = name.split(".csv")[0]
# # # # #     # Remove hash suffix
# # # # #     name = re.sub(r"_[A-Z0-9a-z]{10,}$", "", name)
# # # # #     return re.sub(r"[^a-zA-Z0-9]", "", name).lower()

# # # # # def is_junk_table(name: str) -> bool:
# # # # #     name = name.lower()
# # # # #     if name in ["extract", "csv", "data", "clipboard", "federated"]: return True
# # # # #     if re.match(r"^csv[a-f0-9]{10,}$", name): return True
# # # # #     return False

# # # # # # ============================================================
# # # # # # STEP 1: HYPER METADATA (Backup)
# # # # # # ============================================================
# # # # # def extract_hyper_metadata(hyper_path: str):
# # # # #     tables: Dict[str, List[str]] = {}
# # # # #     with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
# # # # #         with Connection(hyper.endpoint, hyper_path) as conn:
# # # # #             schemas = conn.catalog.get_schema_names()
# # # # #             for schema in schemas:
# # # # #                 for table in conn.catalog.get_table_names(schema):
# # # # #                     raw_name = str(table.name)
# # # # #                     table_name = normalize_table_name(raw_name)
# # # # #                     if is_junk_table(table_name): continue
# # # # #                     cols = []
# # # # #                     try:
# # # # #                         table_def = conn.catalog.get_table_definition(table)
# # # # #                         for c in table_def.columns: cols.append(clean(str(c.name)))
# # # # #                     except Exception: pass
# # # # #                     if cols: tables[table_name] = cols
# # # # #     return tables

# # # # # # ============================================================
# # # # # # STEP 2: XML METADATA (Logical & Mapping)
# # # # # # ============================================================
# # # # # def extract_xml_metadata(root: ET.Element):
# # # # #     xml_tables: Dict[str, List[str]] = {}
    
# # # # #     # Map Local Names (e.g. "[CustomerID (sales.csv)]") to Table/Col info
# # # # #     local_name_map: Dict[str, dict] = {} 

# # # # #     # Search all descendants to catch namespaced tags
# # # # #     all_elements = root.findall(".//")
# # # # #     records = [el for el in all_elements if "metadata-record" in el.tag]

# # # # #     for record in records:
# # # # #         if record.get("class") != "column": continue

# # # # #         remote_name = record.find("remote-name")
# # # # #         parent_name = record.find("parent-name")
# # # # #         local_name_node = record.find("local-name") 
        
# # # # #         if remote_name is not None and parent_name is not None:
# # # # #             col = clean(remote_name.text)
# # # # #             clean_tbl = normalize_table_name(parent_name.text)
            
# # # # #             if is_junk_table(clean_tbl): continue

# # # # #             # Store Table -> Columns
# # # # #             xml_tables.setdefault(clean_tbl, [])
# # # # #             if col not in xml_tables[clean_tbl]:
# # # # #                 xml_tables[clean_tbl].append(col)
                
# # # # #             # Build Local Name Map for Relationship parsing
# # # # #             if local_name_node is not None:
# # # # #                 raw_local = local_name_node.text
# # # # #                 # Store exact raw string (with brackets)
# # # # #                 local_name_map[raw_local] = {"table": clean_tbl, "col": col}
# # # # #                 # Store cleaned version just in case
# # # # #                 local_name_map[clean(raw_local)] = {"table": clean_tbl, "col": col}

# # # # #     return xml_tables, local_name_map

# # # # # # ============================================================
# # # # # # STEP 3: RELATIONSHIPS
# # # # # # ============================================================
# # # # # def extract_relationships(root, valid_tables, local_name_map):
# # # # #     relationships = []
# # # # #     seen = set()
# # # # #     valid_names = set(valid_tables.keys())

# # # # #     def add(from_t, from_c, to_t, to_c):
# # # # #         from_t = normalize_table_name(from_t)
# # # # #         to_t = normalize_table_name(to_t)
        
# # # # #         if from_t not in valid_names or to_t not in valid_names: return
# # # # #         if from_t == to_t: return

# # # # #         key = (from_t, from_c, to_t, to_c)
# # # # #         if key in seen: return
# # # # #         seen.add(key)
        
# # # # #         relationships.append({
# # # # #             "fromTable": from_t,
# # # # #             "fromColumn": from_c,
# # # # #             "toTable": to_t,
# # # # #             "toColumn": to_c,
# # # # #             "relationshipType": "Many-to-One"
# # # # #         })

# # # # #     # Find all relationship tags (namespaced or not)
# # # # #     all_elements = root.findall(".//")
# # # # #     relationship_nodes = [el for el in all_elements if el.tag.endswith("relationship")]
    
# # # # #     for rel in relationship_nodes:
# # # # #         expr = rel.find("expression")
# # # # #         if expr is None: continue
        
# # # # #         # Extract operands: Ignore '=' and focus on [Column] parts
# # # # #         ops = []
# # # # #         for sub_expr in expr.iter("expression"):
# # # # #             op = sub_expr.get("op")
# # # # #             # Only keep operands that look like columns (start with [)
# # # # #             # OR are keys in our map
# # # # #             if op and (op.startswith("[") or op in local_name_map):
# # # # #                 ops.append(op)
            
# # # # #         # We expect exactly 2 operands for a join (ColA, ColB)
# # # # #         if len(ops) == 2:
# # # # #             # Look up both operands in our map
# # # # #             info1 = local_name_map.get(ops[0]) or local_name_map.get(clean(ops[0]))
# # # # #             info2 = local_name_map.get(ops[1]) or local_name_map.get(clean(ops[1]))
            
# # # # #             if info1 and info2:
# # # # #                 add(info1['table'], info1['col'], info2['table'], info2['col'])

# # # # #     return relationships

# # # # # # ============================================================
# # # # # # MAIN ENTRY POINT
# # # # # # ============================================================
# # # # # def extract_metadata_from_twbx(twbx_path: str):
# # # # #     log.info(f"TWBX SIZE: {os.path.getsize(twbx_path)} bytes")
# # # # #     with open(twbx_path, "rb") as f:
# # # # #         log.info(f"TWBX MD5: {hashlib.md5(f.read()).hexdigest()}")

# # # # #     with tempfile.TemporaryDirectory() as tmp:
# # # # #         with zipfile.ZipFile(twbx_path, "r") as z: z.extractall(tmp)

# # # # #         twb = hyper = None
# # # # #         for root_dir, _, files in os.walk(tmp):
# # # # #             for f in files:
# # # # #                 if f.endswith(".twb"): twb = os.path.join(root_dir, f)
# # # # #                 elif f.endswith(".hyper"): hyper = os.path.join(root_dir, f)

# # # # #         if not twb or not hyper: raise ValueError("Invalid TWBX")

# # # # #         tree = ET.parse(twb)
# # # # #         root = tree.getroot()
# # # # #         strip_ns(root)

# # # # #         # 1. XML Extraction (Prioritized)
# # # # #         xml_tables, local_name_map = extract_xml_metadata(root)
        
# # # # #         # 2. Hyper Extraction (Backup)
# # # # #         hyper_tables = extract_hyper_metadata(hyper)
        
# # # # #         # 3. Merge Logic
# # # # #         final_tables = {k: v for k, v in xml_tables.items() if not is_junk_table(k)}
        
# # # # #         for tbl, cols in hyper_tables.items():
# # # # #             if not is_junk_table(tbl) and tbl not in final_tables:
# # # # #                 final_tables[tbl] = cols
# # # # #                 log.info(f"Added table from Hyper: {tbl}")

# # # # #         # 4. Extract Relationships using Local Name Map
# # # # #         relationships = extract_relationships(root, final_tables, local_name_map)

# # # # #         return {"tables": final_tables, "relationships": relationships}




# # # # # --------------------------------------------for live data


# # # # import os
# # # # import re
# # # # import zipfile
# # # # import tempfile
# # # # import logging
# # # # import hashlib
# # # # import xml.etree.ElementTree as ET
# # # # from typing import Dict, List

# # # # from tableauhyperapi import HyperProcess, Telemetry, Connection

# # # # # ============================================================
# # # # # LOGGING
# # # # # ============================================================
# # # # logging.basicConfig(level=logging.INFO)
# # # # log = logging.getLogger("tableau-metadata")

# # # # # ============================================================
# # # # # UTILS
# # # # # ============================================================
# # # # def strip_ns(root: ET.Element):
# # # #     for el in root.iter():
# # # #         if "}" in el.tag:
# # # #             el.tag = el.tag.split("}", 1)[1]

# # # # def clean(val: str) -> str:
# # # #     if not val: return ""
# # # #     return re.sub(r'[\[\]"]', "", val).strip()

# # # # def normalize_table_name(name: str) -> str:
# # # #     name = clean(name)
# # # #     # Remove schema prefix
# # # #     if "." in name and not name.lower().endswith(".csv"):
# # # #         parts = name.split(".")
# # # #         if len(parts) >= 2: name = parts[-1]
# # # #     # Remove tableau internal #
# # # #     if "#" in name: name = name.split("#")[0]
# # # #     # Remove .csv extension
# # # #     if ".csv" in name.lower(): name = name.split(".csv")[0]
# # # #     # Remove hash suffix
# # # #     name = re.sub(r"_[A-Z0-9a-z]{10,}$", "", name)
# # # #     return re.sub(r"[^a-zA-Z0-9]", "", name).lower()

# # # # def is_junk_table(name: str) -> bool:
# # # #     name = name.lower()
# # # #     if name in ["extract", "csv", "data", "clipboard", "federated"]: return True
# # # #     if re.match(r"^csv[a-f0-9]{10,}$", name): return True
# # # #     return False

# # # # # ============================================================
# # # # # STEP 1: HYPER METADATA (Physical Fallback)
# # # # # ============================================================
# # # # def extract_hyper_metadata(hyper_path: str):
# # # #     tables: Dict[str, List[str]] = {}
    
# # # #     # SAFETY CHECK: If hyper file doesn't exist, return empty dict immediately
# # # #     if not hyper_path or not os.path.exists(hyper_path):
# # # #         log.warning("Hyper file not found or invalid. Skipping Hyper extraction.")
# # # #         return tables

# # # #     try:
# # # #         with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
# # # #             with Connection(hyper.endpoint, hyper_path) as conn:
# # # #                 schemas = conn.catalog.get_schema_names()
# # # #                 for schema in schemas:
# # # #                     for table in conn.catalog.get_table_names(schema):
# # # #                         raw_name = str(table.name)
# # # #                         table_name = normalize_table_name(raw_name)
# # # #                         if is_junk_table(table_name): continue
# # # #                         cols = []
# # # #                         try:
# # # #                             table_def = conn.catalog.get_table_definition(table)
# # # #                             for c in table_def.columns: cols.append(clean(str(c.name)))
# # # #                         except Exception: pass
# # # #                         if cols: tables[table_name] = cols
# # # #     except Exception as e:
# # # #         log.warning(f"Hyper extraction failed (ignoring): {e}")
        
# # # #     return tables

# # # # # ============================================================
# # # # # STEP 2: XML METADATA (Logical & Mapping)
# # # # # ============================================================
# # # # def extract_xml_metadata(root: ET.Element):
# # # #     xml_tables: Dict[str, List[str]] = {}
# # # #     local_name_map: Dict[str, dict] = {} 

# # # #     all_elements = root.findall(".//")
# # # #     records = [el for el in all_elements if "metadata-record" in el.tag]

# # # #     for record in records:
# # # #         if record.get("class") != "column": continue

# # # #         remote_name = record.find("remote-name")
# # # #         parent_name = record.find("parent-name")
# # # #         local_name_node = record.find("local-name") 
        
# # # #         if remote_name is not None and parent_name is not None:
# # # #             col = clean(remote_name.text)
# # # #             clean_tbl = normalize_table_name(parent_name.text)
            
# # # #             if is_junk_table(clean_tbl): continue

# # # #             # Store Table -> Columns
# # # #             xml_tables.setdefault(clean_tbl, [])
# # # #             if col not in xml_tables[clean_tbl]:
# # # #                 xml_tables[clean_tbl].append(col)
                
# # # #             # Map Local Names for Relationships
# # # #             if local_name_node is not None:
# # # #                 raw_local = local_name_node.text
# # # #                 local_name_map[raw_local] = {"table": clean_tbl, "col": col}
# # # #                 local_name_map[clean(raw_local)] = {"table": clean_tbl, "col": col}

# # # #     return xml_tables, local_name_map

# # # # # ============================================================
# # # # # STEP 3: RELATIONSHIPS
# # # # # ============================================================
# # # # def extract_relationships(root, valid_tables, local_name_map):
# # # #     relationships = []
# # # #     seen = set()
# # # #     valid_names = set(valid_tables.keys())

# # # #     def add(from_t, from_c, to_t, to_c):
# # # #         from_t = normalize_table_name(from_t)
# # # #         to_t = normalize_table_name(to_t)
        
# # # #         if from_t not in valid_names or to_t not in valid_names: return
# # # #         if from_t == to_t: return

# # # #         key = (from_t, from_c, to_t, to_c)
# # # #         if key in seen: return
# # # #         seen.add(key)
        
# # # #         relationships.append({
# # # #             "fromTable": from_t,
# # # #             "fromColumn": from_c,
# # # #             "toTable": to_t,
# # # #             "toColumn": to_c,
# # # #             "relationshipType": "Many-to-One"
# # # #         })

# # # #     all_elements = root.findall(".//")
# # # #     relationship_nodes = [el for el in all_elements if el.tag.endswith("relationship")]
    
# # # #     for rel in relationship_nodes:
# # # #         expr = rel.find("expression")
# # # #         if expr is None: continue
        
# # # #         ops = []
# # # #         for sub_expr in expr.iter("expression"):
# # # #             op = sub_expr.get("op")
# # # #             if op and (op.startswith("[") or op in local_name_map):
# # # #                 ops.append(op)
            
# # # #         if len(ops) == 2:
# # # #             info1 = local_name_map.get(ops[0]) or local_name_map.get(clean(ops[0]))
# # # #             info2 = local_name_map.get(ops[1]) or local_name_map.get(clean(ops[1]))
            
# # # #             if info1 and info2:
# # # #                 add(info1['table'], info1['col'], info2['table'], info2['col'])

# # # #     return relationships

# # # # # ============================================================
# # # # # MAIN ENTRY POINT
# # # # # ============================================================
# # # # def extract_metadata_from_twbx(file_path: str):
# # # #     is_zip = zipfile.is_zipfile(file_path)
# # # #     log.info(f"Processing: {file_path} (Is Zip: {is_zip})")
    
# # # #     xml_root = None
# # # #     hyper_path = None
# # # #     temp_dir = None

# # # #     try:
# # # #         if is_zip:
# # # #             temp_dir = tempfile.TemporaryDirectory()
# # # #             with zipfile.ZipFile(file_path, "r") as z:
# # # #                 z.extractall(temp_dir.name)
            
# # # #             for root_dir, _, files in os.walk(temp_dir.name):
# # # #                 for f in files:
# # # #                     if f.endswith(".twb"): 
# # # #                         xml_root = ET.parse(os.path.join(root_dir, f)).getroot()
# # # #                     elif f.endswith(".hyper"): 
# # # #                         hyper_path = os.path.join(root_dir, f)
# # # #         else:
# # # #             # Handle raw .twb
# # # #             xml_root = ET.parse(file_path).getroot()
# # # #             hyper_path = None

# # # #         if xml_root is None:
# # # #             raise ValueError("Invalid TWBX: Could not find .twb XML file inside.")

# # # #         strip_ns(xml_root)

# # # #         # 1. XML Extraction (Prioritized)
# # # #         xml_tables, local_name_map = extract_xml_metadata(xml_root)
        
# # # #         # 2. Hyper Extraction (Optional)
# # # #         hyper_tables = extract_hyper_metadata(hyper_path)
        
# # # #         # 3. Merge Logic (Safe even if hyper_tables is empty)
# # # #         final_tables = {k: v for k, v in xml_tables.items() if not is_junk_table(k)}
        
# # # #         for tbl, cols in hyper_tables.items():
# # # #             if not is_junk_table(tbl) and tbl not in final_tables:
# # # #                 final_tables[tbl] = cols
# # # #                 log.info(f"Added table from Hyper: {tbl}")

# # # #         # 4. Extract Relationships
# # # #         relationships = extract_relationships(xml_root, final_tables, local_name_map)

# # # #         return {"tables": final_tables, "relationships": relationships}

# # # #     finally:
# # # #         if temp_dir:
# # # #             temp_dir.cleanup()


# # # import os
# # # import re
# # # import zipfile
# # # import tempfile
# # # import logging
# # # import hashlib
# # # import xml.etree.ElementTree as ET
# # # from typing import Dict, List

# # # from tableauhyperapi import HyperProcess, Telemetry, Connection

# # # # ============================================================
# # # # LOGGING
# # # # ============================================================
# # # logging.basicConfig(level=logging.INFO)
# # # log = logging.getLogger("tableau-metadata")

# # # # ============================================================
# # # # UTILS
# # # # ============================================================
# # # def strip_ns(root: ET.Element):
# # #     for el in root.iter():
# # #         if "}" in el.tag:
# # #             el.tag = el.tag.split("}", 1)[1]

# # # def clean(val: str) -> str:
# # #     if not val: return ""
# # #     return re.sub(r'[\[\]"]', "", val).strip()

# # # def normalize_table_name(name: str) -> str:
# # #     """
# # #     Standardizes table names for CSV, SQL, and Snowflake formats.
# # #     """
# # #     name = clean(name)
    
# # #     # 1. Handle Snowflake/SQL format: "TABLE (DB.SCHEMA.TABLE)"
# # #     # This removes the parentheses and everything inside them
# # #     name = re.sub(r"\s*\(.*?\)", "", name)

# # #     # 2. Remove Schema Prefixes (e.g. [dbo].[Customers] -> Customers)
# # #     if "." in name and not name.lower().endswith(".csv"):
# # #         parts = name.split(".")
# # #         if len(parts) >= 2:
# # #             name = parts[-1] 

# # #     # 3. Handle Tableau internal naming (table#csv)
# # #     if "#" in name:
# # #         name = name.split("#")[0]

# # #     # 4. Remove .csv extension
# # #     if ".csv" in name.lower():
# # #         name = name.split(".csv")[0]

# # #     # 5. Remove Hash Suffix (Underscore + long hex)
# # #     name = re.sub(r"_[A-Z0-9a-z]{10,}$", "", name)
        
# # #     return re.sub(r"[^a-zA-Z0-9]", "", name).lower()

# # # def is_junk_table(name: str) -> bool:
# # #     name = name.lower()
# # #     if name in ["extract", "csv", "data", "clipboard", "federated"]: return True
# # #     if re.match(r"^csv[a-f0-9]{10,}$", name): return True
# # #     return False

# # # # ============================================================
# # # # STEP 1: HYPER METADATA (Physical Fallback)
# # # # ============================================================
# # # def extract_hyper_metadata(hyper_path: str):
# # #     tables: Dict[str, List[str]] = {}
    
# # #     # SAFETY CHECK: If hyper file doesn't exist, return empty dict immediately
# # #     if not hyper_path or not os.path.exists(hyper_path):
# # #         return tables

# # #     try:
# # #         with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
# # #             with Connection(hyper.endpoint, hyper_path) as conn:
# # #                 schemas = conn.catalog.get_schema_names()
# # #                 for schema in schemas:
# # #                     for table in conn.catalog.get_table_names(schema):
# # #                         raw_name = str(table.name)
# # #                         table_name = normalize_table_name(raw_name)
# # #                         if is_junk_table(table_name): continue
# # #                         cols = []
# # #                         try:
# # #                             table_def = conn.catalog.get_table_definition(table)
# # #                             for c in table_def.columns: cols.append(clean(str(c.name)))
# # #                         except Exception: pass
# # #                         if cols: tables[table_name] = cols
# # #     except Exception as e:
# # #         log.warning(f"Hyper extraction failed (ignoring): {e}")
        
# # #     return tables

# # # # ============================================================
# # # # STEP 2: XML METADATA (Logical & Mapping)
# # # # ============================================================
# # # def extract_xml_metadata(root: ET.Element):
# # #     xml_tables: Dict[str, List[str]] = {}
# # #     local_name_map: Dict[str, dict] = {} 

# # #     all_elements = root.findall(".//")
# # #     records = [el for el in all_elements if "metadata-record" in el.tag]

# # #     for record in records:
# # #         if record.get("class") != "column": continue

# # #         remote_name = record.find("remote-name")
# # #         parent_name = record.find("parent-name")
# # #         local_name_node = record.find("local-name") 
        
# # #         if remote_name is not None and parent_name is not None:
# # #             col = clean(remote_name.text)
# # #             clean_tbl = normalize_table_name(parent_name.text)
            
# # #             if is_junk_table(clean_tbl): continue

# # #             # Store Table -> Columns
# # #             xml_tables.setdefault(clean_tbl, [])
# # #             if col not in xml_tables[clean_tbl]:
# # #                 xml_tables[clean_tbl].append(col)
                
# # #             # Map Local Names for Relationships
# # #             if local_name_node is not None:
# # #                 raw_local = local_name_node.text
# # #                 local_name_map[raw_local] = {"table": clean_tbl, "col": col}
# # #                 local_name_map[clean(raw_local)] = {"table": clean_tbl, "col": col}

# # #     return xml_tables, local_name_map

# # # # ============================================================
# # # # STEP 3: RELATIONSHIPS
# # # # ============================================================
# # # def extract_relationships(root, valid_tables, local_name_map):
# # #     relationships = []
# # #     seen = set()
# # #     valid_names = set(valid_tables.keys())

# # #     def add(from_t, from_c, to_t, to_c):
# # #         from_t = normalize_table_name(from_t)
# # #         to_t = normalize_table_name(to_t)
        
# # #         if from_t not in valid_names or to_t not in valid_names: return
# # #         if from_t == to_t: return

# # #         key = (from_t, from_c, to_t, to_c)
# # #         if key in seen: return
# # #         seen.add(key)
        
# # #         relationships.append({
# # #             "fromTable": from_t,
# # #             "fromColumn": from_c,
# # #             "toTable": to_t,
# # #             "toColumn": to_c,
# # #             "relationshipType": "Many-to-One"
# # #         })

# # #     all_elements = root.findall(".//")
# # #     relationship_nodes = [el for el in all_elements if el.tag.endswith("relationship")]
    
# # #     for rel in relationship_nodes:
# # #         expr = rel.find("expression")
# # #         if expr is None: continue
        
# # #         ops = []
# # #         for sub_expr in expr.iter("expression"):
# # #             op = sub_expr.get("op")
# # #             if op and (op.startswith("[") or op in local_name_map):
# # #                 ops.append(op)
            
# # #         if len(ops) == 2:
# # #             info1 = local_name_map.get(ops[0]) or local_name_map.get(clean(ops[0]))
# # #             info2 = local_name_map.get(ops[1]) or local_name_map.get(clean(ops[1]))
            
# # #             if info1 and info2:
# # #                 add(info1['table'], info1['col'], info2['table'], info2['col'])

# # #     return relationships

# # # # ============================================================
# # # # MAIN ENTRY POINT
# # # # ============================================================
# # # def extract_metadata_from_twbx(file_path: str):
# # #     is_zip = zipfile.is_zipfile(file_path)
# # #     log.info(f"Processing: {file_path} (Is Zip: {is_zip})")
    
# # #     xml_root = None
# # #     hyper_path = None
# # #     temp_dir = None

# # #     try:
# # #         if is_zip:
# # #             temp_dir = tempfile.TemporaryDirectory()
# # #             with zipfile.ZipFile(file_path, "r") as z:
# # #                 z.extractall(temp_dir.name)
            
# # #             for root_dir, _, files in os.walk(temp_dir.name):
# # #                 for f in files:
# # #                     if f.endswith(".twb"): 
# # #                         xml_root = ET.parse(os.path.join(root_dir, f)).getroot()
# # #                     elif f.endswith(".hyper"): 
# # #                         hyper_path = os.path.join(root_dir, f)
# # #         else:
# # #             # Handle raw .twb
# # #             xml_root = ET.parse(file_path).getroot()
# # #             hyper_path = None

# # #         if xml_root is None:
# # #             raise ValueError("Invalid TWBX: Could not find .twb XML file inside.")

# # #         strip_ns(xml_root)

# # #         # 1. XML Extraction (Prioritized)
# # #         xml_tables, local_name_map = extract_xml_metadata(xml_root)
        
# # #         # 2. Hyper Extraction (Optional)
# # #         hyper_tables = {}
# # #         if hyper_path:
# # #             hyper_tables = extract_hyper_metadata(hyper_path)
        
# # #         # 3. Merge Logic
# # #         final_tables = {k: v for k, v in xml_tables.items() if not is_junk_table(k)}
        
# # #         for tbl, cols in hyper_tables.items():
# # #             if not is_junk_table(tbl) and tbl not in final_tables:
# # #                 final_tables[tbl] = cols
# # #                 log.info(f"Added table from Hyper: {tbl}")

# # #         # 4. Extract Relationships
# # #         relationships = extract_relationships(xml_root, final_tables, local_name_map)

# # #         return {"tables": final_tables, "relationships": relationships}

# # #     finally:
# # #         if temp_dir:
# # #             temp_dir.cleanup()


# # # ---------------visual added 



# # import os
# # import re
# # import zipfile
# # import tempfile
# # import logging
# # import hashlib
# # import xml.etree.ElementTree as ET
# # from typing import Dict, List, Any

# # from tableauhyperapi import HyperProcess, Telemetry, Connection

# # # ============================================================
# # # LOGGING
# # # ============================================================
# # logging.basicConfig(level=logging.INFO)
# # log = logging.getLogger("tableau-metadata")

# # # ============================================================
# # # CONSTANTS (Visual Mappings)
# # # ============================================================
# # MARK_MAP = {
# #     'bar': 'Bar Chart',
# #     'line': 'Line Chart',
# #     'area': 'Area Chart',
# #     'text': 'Text Table',
# #     'circle': 'Scatter Plot',
# #     'square': 'Heat Map',
# #     'pie': 'Pie Chart',
# #     'map': 'Map',
# #     'ganttbar': 'Gantt Chart',
# #     'shape': 'Shape Chart',
# #     'scatter': 'Scatter Plot',
# #     'multipolygon': 'Map',
# #     'filledmap': 'Map'
# # }

# # # ============================================================
# # # UTILS
# # # ============================================================
# # def strip_ns(root: ET.Element):
# #     for el in root.iter():
# #         if "}" in el.tag:
# #             el.tag = el.tag.split("}", 1)[1]

# # def clean(val: str) -> str:
# #     if not val: return ""
# #     return re.sub(r'[\[\]"]', "", val).strip()

# # def clean_visual_column_name(name: str) -> str:
# #     """
# #     Cleans column names found in worksheet dependencies.
# #     Examples: 
# #     - "[sum:Sales:qk]" -> "Sales"
# #     - "[none:CustomerID:ok]" -> "CustomerID"
# #     """
# #     if not name: return ""
# #     name = name.replace("[", "").replace("]", "")
# #     # Remove Tableau prefixes (sum:, avg:, none:, etc.)
# #     name = re.sub(r'^(none|sum|avg|min|max|count|attr|yr|mn|dy|qd|tdc):', '', name, flags=re.IGNORECASE)
# #     # Remove Tableau suffixes (:qk, :ok, :nk)
# #     name = re.sub(r':(nk|ok|qk|sk)$', '', name, flags=re.IGNORECASE)
# #     return name.strip()

# # def normalize_table_name(name: str) -> str:
# #     name = clean(name)
# #     if "." in name and not name.lower().endswith(".csv"):
# #         parts = name.split(".")
# #         if len(parts) >= 2: name = parts[-1]
# #     if "#" in name: name = name.split("#")[0]
# #     if ".csv" in name.lower(): name = name.split(".csv")[0]
# #     name = re.sub(r"_[A-Z0-9a-z]{10,}$", "", name)
# #     return re.sub(r"[^a-zA-Z0-9]", "", name).lower()

# # def is_junk_table(name: str) -> bool:
# #     name = name.lower()
# #     if name in ["extract", "csv", "data", "clipboard", "federated"]: return True
# #     if re.match(r"^csv[a-f0-9]{10,}$", name): return True
# #     return False

# # # ============================================================
# # # STEP 1: HYPER METADATA (Physical Fallback)
# # # ============================================================
# # def extract_hyper_metadata(hyper_path: str):
# #     tables: Dict[str, List[str]] = {}
    
# #     if not hyper_path or not os.path.exists(hyper_path):
# #         return tables

# #     try:
# #         with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
# #             with Connection(hyper.endpoint, hyper_path) as conn:
# #                 schemas = conn.catalog.get_schema_names()
# #                 for schema in schemas:
# #                     for table in conn.catalog.get_table_names(schema):
# #                         raw_name = str(table.name)
# #                         table_name = normalize_table_name(raw_name)
# #                         if is_junk_table(table_name): continue
# #                         cols = []
# #                         try:
# #                             table_def = conn.catalog.get_table_definition(table)
# #                             for c in table_def.columns: cols.append(clean(str(c.name)))
# #                         except Exception: pass
# #                         if cols: tables[table_name] = cols
# #     except Exception as e:
# #         log.warning(f"Hyper extraction failed: {e}")
        
# #     return tables

# # # ============================================================
# # # STEP 2: XML METADATA (Logical & Mapping)
# # # ============================================================
# # def extract_xml_metadata(root: ET.Element):
# #     xml_tables: Dict[str, List[str]] = {}
# #     local_name_map: Dict[str, dict] = {} 

# #     all_elements = root.findall(".//")
# #     records = [el for el in all_elements if "metadata-record" in el.tag]

# #     for record in records:
# #         if record.get("class") != "column": continue

# #         remote_name = record.find("remote-name")
# #         parent_name = record.find("parent-name")
# #         local_name_node = record.find("local-name") 
        
# #         if remote_name is not None and parent_name is not None:
# #             col = clean(remote_name.text)
# #             clean_tbl = normalize_table_name(parent_name.text)
            
# #             if is_junk_table(clean_tbl): continue

# #             xml_tables.setdefault(clean_tbl, [])
# #             if col not in xml_tables[clean_tbl]:
# #                 xml_tables[clean_tbl].append(col)
                
# #             if local_name_node is not None:
# #                 raw_local = local_name_node.text
# #                 local_name_map[raw_local] = {"table": clean_tbl, "col": col}
# #                 local_name_map[clean(raw_local)] = {"table": clean_tbl, "col": col}

# #     return xml_tables, local_name_map

# # # ============================================================
# # # STEP 3: RELATIONSHIPS
# # # ============================================================
# # def extract_relationships(root, valid_tables, local_name_map):
# #     relationships = []
# #     seen = set()
# #     valid_names = set(valid_tables.keys())

# #     def add(from_t, from_c, to_t, to_c):
# #         from_t = normalize_table_name(from_t)
# #         to_t = normalize_table_name(to_t)
        
# #         if from_t not in valid_names or to_t not in valid_names: return
# #         if from_t == to_t: return

# #         key = (from_t, from_c, to_t, to_c)
# #         if key in seen: return
# #         seen.add(key)
        
# #         relationships.append({
# #             "fromTable": from_t,
# #             "fromColumn": from_c,
# #             "toTable": to_t,
# #             "toColumn": to_c,
# #             "relationshipType": "Many-to-One"
# #         })

# #     all_elements = root.findall(".//")
# #     relationship_nodes = [el for el in all_elements if el.tag.endswith("relationship")]
    
# #     for rel in relationship_nodes:
# #         expr = rel.find("expression")
# #         if expr is None: continue
        
# #         ops = []
# #         for sub_expr in expr.iter("expression"):
# #             op = sub_expr.get("op")
# #             if op and (op.startswith("[") or op in local_name_map):
# #                 ops.append(op)
            
# #         if len(ops) == 2:
# #             info1 = local_name_map.get(ops[0]) or local_name_map.get(clean(ops[0]))
# #             info2 = local_name_map.get(ops[1]) or local_name_map.get(clean(ops[1]))
            
# #             if info1 and info2:
# #                 add(info1['table'], info1['col'], info2['table'], info2['col'])

# #     return relationships

# # # ============================================================
# # # STEP 4: VISUALS (Worksheets & Dashboards)
# # # ============================================================
# # def extract_visuals(root: ET.Element, valid_tables: Dict[str, List[str]]):
# #     worksheets = []
# #     dashboards = []

# #     # 1. Build a Reverse Map: Column Name -> Table Name
# #     # This fixes the "MainTable" flaw by finding the REAL table.
# #     col_to_table = {}
# #     for tbl, cols in valid_tables.items():
# #         for c in cols:
# #             # Note: If a column name exists in multiple tables (e.g. ID), this simple map
# #             # picks the last one seen. For visualization context, this is usually acceptable approximation.
# #             col_to_table[c.lower()] = tbl

# #     # 2. Extract Worksheets
# #     for worksheet in root.findall(".//worksheet"):
# #         sheet_name = worksheet.get('name')
# #         bound_columns_set = set()

# #         # Dependencies (Columns used in the sheet)
# #         for dep in worksheet.findall(".//datasource-dependencies"):
# #             for col in dep.findall("column-instance"):
# #                 col_ref = col.get('column')
# #                 raw_name = col.get('name')
                
# #                 # Try to clean the name from the reference or the name attribute
# #                 final_col = None
# #                 if col_ref:
# #                     # format [table].[col] or just [col]
# #                     parts = col_ref.split(']:')
# #                     if len(parts) > 1:
# #                         final_col = clean_visual_column_name(parts[-1])
                
# #                 if not final_col and raw_name:
# #                     final_col = clean_visual_column_name(raw_name)
                
# #                 if final_col:
# #                     bound_columns_set.add(final_col)

# #         # Visual Type Detection
# #         visual_type = "Automatic"
        
# #         # A. Check explicit mark type
# #         for mark_element in worksheet.findall(".//pane/mark"):
# #             cls = mark_element.get('class')
# #             if cls and cls != "Automatic":
# #                 visual_type = MARK_MAP.get(cls.lower(), cls.capitalize())
# #                 break
        
# #         # B. Heuristics if Automatic
# #         if visual_type == "Automatic":
# #             if worksheet.find(".//style-rule[@element='map']") is not None:
# #                 visual_type = "Map"
# #             elif worksheet.find(".//style-rule[@element='table']") is not None:
# #                 visual_type = "Text Table"
# #             else:
# #                 # Column name guessing
# #                 col_list_lower = [c.lower() for c in bound_columns_set]
# #                 map_keywords = ['lat', 'lon', 'country', 'city', 'state', 'zip', 'geo']
# #                 if any(k in c for c in col_list_lower for k in map_keywords):
# #                     visual_type = "Map"
# #                 elif len(bound_columns_set) >= 1:
# #                     visual_type = "Bar Chart" # Default guess for generic charts

# #         # Format Columns with REAL Table Names
# #         formatted_columns = []
# #         for col in sorted(list(bound_columns_set)):
# #             # Lookup the real table
# #             real_table = col_to_table.get(col.lower(), "Calculated Field")
# #             formatted_columns.append({
# #                 "table": real_table,
# #                 "column": col
# #             })

# #         worksheets.append({
# #             "name": sheet_name,
# #             "visualType": visual_type,
# #             "columns": formatted_columns
# #         })

# #     # 3. Extract Dashboards
# #     for dashboard in root.findall(".//dashboard"):
# #         db_name = dashboard.get("name")
# #         ws_in_db = []
# #         # Find zones that reference worksheets
# #         for zone in dashboard.findall(".//zone"):
# #             z_name = zone.get("name")
# #             # Filter out layout containers, only keep sheets that exist in our worksheet list
# #             if z_name and any(ws['name'] == z_name for ws in worksheets):
# #                 ws_in_db.append(z_name)
        
# #         dashboards.append({
# #             "dashboardName": db_name,
# #             "worksheets": list(set(ws_in_db))
# #         })

# #     return worksheets, dashboards

# # # ============================================================
# # # MAIN ENTRY POINT
# # # ============================================================
# # def extract_metadata_from_twbx(file_path: str):
# #     log.info(f"TWBX SIZE: {os.path.getsize(file_path)} bytes")
# #     with open(file_path, "rb") as f:
# #         log.info(f"TWBX MD5: {hashlib.md5(f.read()).hexdigest()}")

# #     with tempfile.TemporaryDirectory() as tmp:
# #         is_zip = zipfile.is_zipfile(file_path)
# #         xml_root = None
# #         hyper_path = None

# #         if is_zip:
# #             with zipfile.ZipFile(file_path, "r") as z: z.extractall(tmp)
# #             for root_dir, _, files in os.walk(tmp):
# #                 for f in files:
# #                     if f.endswith(".twb"): xml_root = ET.parse(os.path.join(root_dir, f)).getroot()
# #                     elif f.endswith(".hyper"): hyper_path = os.path.join(root_dir, f)
# #         else:
# #             xml_root = ET.parse(file_path).getroot()

# #         if xml_root is None: raise ValueError("Invalid TWBX/TWB")

# #         strip_ns(xml_root)

# #         # 1. Tables & Columns
# #         xml_tables, local_name_map = extract_xml_metadata(xml_root)
# #         hyper_tables = extract_hyper_metadata(hyper_path)
        
# #         final_tables = {k: v for k, v in xml_tables.items() if not is_junk_table(k)}
# #         for tbl, cols in hyper_tables.items():
# #             if not is_junk_table(tbl) and tbl not in final_tables:
# #                 final_tables[tbl] = cols

# #         # 2. Relationships
# #         relationships = extract_relationships(xml_root, final_tables, local_name_map)

# #         # 3. Visuals (NEW)
# #         worksheets, dashboards = extract_visuals(xml_root, final_tables)

# #         return {
# #             "tables": final_tables,
# #             "relationships": relationships,
# #             "worksheets": worksheets,
# #             "dashboards": dashboards
# #         }


# import os
# import re
# import zipfile
# import tempfile
# import logging
# import hashlib
# import xml.etree.ElementTree as ET
# from typing import Dict, List, Any

# from tableauhyperapi import HyperProcess, Telemetry, Connection

# # ============================================================
# # LOGGING
# # ============================================================
# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger("tableau-metadata")

# # ============================================================
# # CONSTANTS (Visual Mapping)
# # ============================================================
# MARK_MAP = {
#     'bar': 'Bar Chart',
#     'line': 'Line Chart',
#     'area': 'Area Chart',
#     'text': 'Text Table',
#     'circle': 'Scatter Plot',
#     'square': 'Heat Map',
#     'pie': 'Pie Chart',
#     'map': 'Map',
#     'ganttbar': 'Gantt Chart',
#     'shape': 'Shape Chart',
#     'scatter': 'Scatter Plot',
#     'multipolygon': 'Map',
#     'filledmap': 'Map'
# }

# # ============================================================
# # UTILS
# # ============================================================
# def strip_ns(root: ET.Element):
#     for el in root.iter():
#         if "}" in el.tag:
#             el.tag = el.tag.split("}", 1)[1]

# def clean(val: str) -> str:
#     if not val: return ""
#     return re.sub(r'[\[\]"]', "", val).strip()

# def normalize_table_name(name: str) -> str:
#     name = clean(name)
#     # Handle SQL/Snowflake format: "TABLE (SCHEMA.TABLE)"
#     name = re.sub(r"\s*\(.*?\)", "", name)
    
#     if "." in name and not name.lower().endswith(".csv"):
#         parts = name.split(".")
#         if len(parts) >= 2: name = parts[-1]
#     if "#" in name: name = name.split("#")[0]
#     if ".csv" in name.lower(): name = name.split(".csv")[0]
#     name = re.sub(r"_[A-Z0-9a-z]{10,}$", "", name)
#     return re.sub(r"[^a-zA-Z0-9]", "", name).lower()

# def is_junk_table(name: str) -> bool:
#     name = name.lower()
#     if name in ["extract", "csv", "data", "clipboard", "federated"]: return True
#     if re.match(r"^csv[a-f0-9]{10,}$", name): return True
#     return False

# def clean_visual_column_name(name: str) -> str:
#     """Cleans internal Tableau column names for visual metadata."""
#     if not name: return ""
#     name = name.replace("[", "").replace("]", "")
#     # Remove tableau internal prefixes (sum:, none:, etc.)
#     name = re.sub(r'^(none|sum|avg|min|max|count|attr|yr|mn|dy|qd|tdc):', '', name, flags=re.IGNORECASE)
#     # Remove internal suffixes
#     name = re.sub(r':(nk|ok|qk|sk)$', '', name, flags=re.IGNORECASE)
    
#     # CRITICAL FIX: Remove table aliases in parentheses 
#     # e.g. "ProductID (products.csv)" -> "ProductID"
#     name = re.sub(r"\s*\(.*?\)", "", name)
    
#     return name.strip()

# # ============================================================
# # STEP 1: HYPER METADATA (Physical)
# # ============================================================
# def extract_hyper_metadata(hyper_path: str):
#     tables: Dict[str, List[str]] = {}
#     if not hyper_path or not os.path.exists(hyper_path): return tables

#     try:
#         with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
#             with Connection(hyper.endpoint, hyper_path) as conn:
#                 schemas = conn.catalog.get_schema_names()
#                 for schema in schemas:
#                     for table in conn.catalog.get_table_names(schema):
#                         raw_name = str(table.name)
#                         table_name = normalize_table_name(raw_name)
#                         if is_junk_table(table_name): continue
#                         cols = []
#                         try:
#                             table_def = conn.catalog.get_table_definition(table)
#                             for c in table_def.columns: cols.append(clean(str(c.name)))
#                         except Exception: pass
#                         if cols: tables[table_name] = cols
#     except Exception as e:
#         log.warning(f"Hyper extraction failed: {e}")
#     return tables

# # ============================================================
# # STEP 2: XML METADATA (Logical)
# # ============================================================
# def extract_xml_metadata(root: ET.Element):
#     xml_tables: Dict[str, List[str]] = {}
#     local_name_map: Dict[str, dict] = {} 

#     all_elements = root.findall(".//")
#     records = [el for el in all_elements if "metadata-record" in el.tag]

#     for record in records:
#         if record.get("class") != "column": continue
#         remote_name = record.find("remote-name")
#         parent_name = record.find("parent-name")
#         local_name_node = record.find("local-name") 
        
#         if remote_name is not None and parent_name is not None:
#             col = clean(remote_name.text)
#             clean_tbl = normalize_table_name(parent_name.text)
#             if is_junk_table(clean_tbl): continue

#             xml_tables.setdefault(clean_tbl, [])
#             if col not in xml_tables[clean_tbl]: xml_tables[clean_tbl].append(col)
                
#             if local_name_node is not None:
#                 raw_local = local_name_node.text
#                 local_name_map[raw_local] = {"table": clean_tbl, "col": col}
#                 local_name_map[clean(raw_local)] = {"table": clean_tbl, "col": col}

#     return xml_tables, local_name_map

# # ============================================================
# # STEP 3: RELATIONSHIPS
# # ============================================================
# def extract_relationships(root, valid_tables, local_name_map):
#     relationships = []
#     seen = set()
#     valid_names = set(valid_tables.keys())

#     def add(from_t, from_c, to_t, to_c):
#         from_t = normalize_table_name(from_t)
#         to_t = normalize_table_name(to_t)
#         if from_t not in valid_names or to_t not in valid_names: return
#         if from_t == to_t: return
#         key = (from_t, from_c, to_t, to_c)
#         if key in seen: return
#         seen.add(key)
#         relationships.append({
#             "fromTable": from_t,
#             "fromColumn": from_c,
#             "toTable": to_t,
#             "toColumn": to_c,
#             "relationshipType": "Many-to-One"
#         })

#     all_elements = root.findall(".//")
#     relationship_nodes = [el for el in all_elements if el.tag.endswith("relationship")]
    
#     for rel in relationship_nodes:
#         expr = rel.find("expression")
#         if expr is None: continue
#         ops = []
#         for sub_expr in expr.iter("expression"):
#             op = sub_expr.get("op")
#             if op and (op.startswith("[") or op in local_name_map): ops.append(op)
            
#         if len(ops) == 2:
#             info1 = local_name_map.get(ops[0]) or local_name_map.get(clean(ops[0]))
#             info2 = local_name_map.get(ops[1]) or local_name_map.get(clean(ops[1]))
#             if info1 and info2:
#                 add(info1['table'], info1['col'], info2['table'], info2['col'])

#     return relationships

# # ============================================================
# # STEP 4: VISUAL METADATA (FIXED)
# # ============================================================
# def extract_visual_metadata(root: ET.Element, column_map: Dict[str, List[str]]):
#     worksheets_data = []
#     dashboards_data = []

#     # A. Worksheets
#     for worksheet in root.findall(".//worksheet"):
#         sheet_name = worksheet.get('name')
#         bound_columns_set = set()

#         for dep in worksheet.findall(".//datasource-dependencies"):
#             for col in dep.findall("column-instance"):
#                 col_ref = col.get('column')
#                 clean_col = None
                
#                 if col_ref and ']:' in col_ref:
#                     parts = col_ref.split(']:')
#                     if len(parts) > 1: clean_col = clean_visual_column_name(parts[-1])
                
#                 if not clean_col:
#                     clean_col = clean_visual_column_name(col.get('name'))
                    
#                 if clean_col: bound_columns_set.add(clean_col)

#         visual_type = "Automatic"
#         for mark_element in worksheet.findall(".//pane/mark"):
#             cls = mark_element.get('class')
#             if cls and cls != "Automatic":
#                 visual_type = MARK_MAP.get(cls.lower(), cls.capitalize())
#                 break
        
#         if visual_type == "Automatic":
#             if worksheet.find(".//style-rule[@element='map']") is not None: visual_type = "Map"
#             elif worksheet.find(".//style-rule[@element='table']") is not None: visual_type = "Text Table"
#             else:
#                 col_list_lower = [c.lower() for c in bound_columns_set]
#                 map_keywords = ['lat', 'lon', 'country', 'city', 'state', 'zip', 'geo']
#                 if any(k in col for col in col_list_lower for k in map_keywords): visual_type = "Map"
#                 elif len(bound_columns_set) == 1: visual_type = "Text Table"
#                 else: visual_type = "Bar Chart"

#         formatted_columns = []
#         for col in sorted(list(bound_columns_set)):
#             possible_tables = column_map.get(col, ["Unknown"])
#             table_name = possible_tables[0] if possible_tables else "Unknown"
#             formatted_columns.append({"table": table_name, "column": col})

#         worksheets_data.append({
#             "name": sheet_name,
#             "visualType": visual_type,
#             "columns": formatted_columns
#         })

#     # B. Dashboards
#     for dashboard in root.findall(".//dashboard"):
#         ws_names = []
#         for zone in dashboard.findall(".//zone"):
#             z_name = zone.get("name")
#             if z_name: ws_names.append(z_name)
        
#         dashboards_data.append({
#             "dashboardName": dashboard.get("name"),
#             "worksheets": list(set(ws_names))
#         })

#     return worksheets_data, dashboards_data

# # ============================================================
# # MAIN ENTRY POINT
# # ============================================================
# def extract_metadata_from_twbx(twbx_path: str):
#     log.info(f"TWBX SIZE: {os.path.getsize(twbx_path)} bytes")
#     with open(twbx_path, "rb") as f:
#         log.info(f"TWBX MD5: {hashlib.md5(f.read()).hexdigest()}")

#     with tempfile.TemporaryDirectory() as tmp:
#         with zipfile.ZipFile(twbx_path, "r") as z: z.extractall(tmp)

#         twb = hyper = None
#         for root_dir, _, files in os.walk(tmp):
#             for f in files:
#                 if f.endswith(".twb"): twb = os.path.join(root_dir, f)
#                 elif f.endswith(".hyper"): hyper = os.path.join(root_dir, f)

#         if not twb: raise ValueError("Invalid TWBX: No .twb found")

#         tree = ET.parse(twb)
#         root = tree.getroot()
#         strip_ns(root)

#         # 1. Data Model Extraction
#         xml_tables, local_name_map = extract_xml_metadata(root)
#         hyper_tables = {}
#         if hyper: hyper_tables = extract_hyper_metadata(hyper)
        
#         final_tables = {k: v for k, v in xml_tables.items() if not is_junk_table(k)}
#         for tbl, cols in hyper_tables.items():
#             if not is_junk_table(tbl) and tbl not in final_tables:
#                 final_tables[tbl] = cols

#         relationships = extract_relationships(root, final_tables, local_name_map)

#         # 2. Build Column Map
#         column_map = {}
#         for tbl, cols in final_tables.items():
#             for c in cols: column_map.setdefault(c, []).append(tbl)

#         # 3. Visual Metadata
#         worksheets, dashboards = extract_visual_metadata(root, column_map)

#         return {
#             "tables": final_tables,
#             "relationships": relationships,
#             "worksheets": worksheets,
#             "dashboards": dashboards
#         }




import os
import re
import zipfile
import tempfile
import logging
import hashlib
import xml.etree.ElementTree as ET
from typing import Dict, List, Any

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tableau-metadata")

# ============================================================
# CONSTANTS (Visual Mapping)
# ============================================================
MARK_MAP = {
    'bar': 'Bar Chart',
    'line': 'Line Chart',
    'area': 'Area Chart',
    'text': 'Text Table',
    'circle': 'Scatter Plot',
    'square': 'Heat Map',
    'pie': 'Pie Chart',
    'map': 'Map',
    'ganttbar': 'Gantt Chart',
    'shape': 'Shape Chart',
    'scatter': 'Scatter Plot',
    'multipolygon': 'Map',
    'filledmap': 'Map'
}

# ============================================================
# UTILS
# ============================================================
def strip_ns(root: ET.Element):
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]

def clean(val: str) -> str:
    if not val: return ""
    return re.sub(r'[\[\]"]', "", val).strip()

def normalize_table_name(name: str) -> str:
    """
    UPDATED: Aggressively cleans table names by removing 'Extract' prefixes 
    and internal Tableau UUIDs/hexadecimal strings.
    """
    name = clean(name)
    # Handle SQL/Snowflake format: "TABLE (SCHEMA.TABLE)"
    name = re.sub(r"\s*\(.*?\)", "", name)
    
    # NEW: Remove 'Extract' prefix and common file suffixes
    name = re.sub(r'^Extract[_\s]?', '', name, flags=re.IGNORECASE)
    
    if "." in name and not name.lower().endswith(".csv"):
        parts = name.split(".")
        if len(parts) >= 2: name = parts[-1]
    if "#" in name: name = name.split("#")[0]
    if ".csv" in name.lower(): name = name.split(".csv")[0]
    
    # NEW: Specific regex to strip 32-character hex strings common in extracts
    name = re.sub(r"_[A-Z0-9a-z]{32}$", "", name)
    name = re.sub(r"_[A-Z0-9a-z]{10,}$", "", name)
    
    return re.sub(r"[^a-zA-Z0-9]", "", name).lower()

def is_junk_table(name: str) -> bool:
    name = name.lower()
    if name in ["extract", "csv", "data", "clipboard", "federated"]: return True
    if re.match(r"^csv[a-f0-9]{10,}$", name): return True
    return False

def clean_visual_column_name(name: str) -> str:
    """Cleans internal Tableau column names for visual metadata."""
    if not name: return ""
    name = name.replace("[", "").replace("]", "")
    # Remove tableau internal prefixes (sum:, none:, etc.)
    name = re.sub(r'^(none|sum|avg|min|max|count|attr|yr|mn|dy|qd|tdc):', '', name, flags=re.IGNORECASE)
    # Remove internal suffixes
    name = re.sub(r':(nk|ok|qk|sk)$', '', name, flags=re.IGNORECASE)
    
    # CRITICAL FIX: Remove table aliases in parentheses 
    # e.g. "ProductID (products.csv)" -> "ProductID"
    name = re.sub(r"\s*\(.*?\)", "", name)
    
    return name.strip()

# ============================================================
# STEP 1: HYPER METADATA (Physical)
# ============================================================
def extract_hyper_metadata(hyper_path: str):
    # This remains the same, but will benefit from the updated normalize_table_name
    tables: Dict[str, List[str]] = {}
    if not hyper_path or not os.path.exists(hyper_path): return tables

    try:
        from tableauhyperapi import HyperProcess, Telemetry, Connection
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, hyper_path) as conn:
                schemas = conn.catalog.get_schema_names()
                for schema in schemas:
                    for table in conn.catalog.get_table_names(schema):
                        raw_name = str(table.name)
                        table_name = normalize_table_name(raw_name)
                        if is_junk_table(table_name): continue
                        cols = []
                        try:
                            table_def = conn.catalog.get_table_definition(table)
                            for c in table_def.columns: cols.append(clean(str(c.name)))
                        except Exception: pass
                        if cols: tables[table_name] = cols
    except Exception as e:
        log.warning(f"Hyper extraction failed: {e}")
    return tables

# ============================================================
# STEP 2: XML METADATA (Logical)
# ============================================================
def extract_xml_metadata(root: ET.Element):
    xml_tables: Dict[str, List[str]] = {}
    local_name_map: Dict[str, dict] = {} 

    all_elements = root.findall(".//")
    records = [el for el in all_elements if "metadata-record" in el.tag]

    for record in records:
        if record.get("class") != "column": continue
        remote_name = record.find("remote-name")
        parent_name = record.find("parent-name")
        local_name_node = record.find("local-name") 
        
        if remote_name is not None and parent_name is not None:
            col = clean(remote_name.text)
            clean_tbl = normalize_table_name(parent_name.text)
            if is_junk_table(clean_tbl): continue

            xml_tables.setdefault(clean_tbl, [])
            if col not in xml_tables[clean_tbl]: xml_tables[clean_tbl].append(col)
                
            if local_name_node is not None:
                raw_local = local_name_node.text
                local_name_map[raw_local] = {"table": clean_tbl, "col": col}
                local_name_map[clean(raw_local)] = {"table": clean_tbl, "col": col}

    return xml_tables, local_name_map

# ============================================================
# STEP 3: RELATIONSHIPS
# ============================================================
def extract_relationships(root, valid_tables, local_name_map):
    relationships = []
    seen = set()
    valid_names = set(valid_tables.keys())

    def add(from_t, from_c, to_t, to_c):
        from_t = normalize_table_name(from_t)
        to_t = normalize_table_name(to_t)
        if from_t not in valid_names or to_t not in valid_names: return
        if from_t == to_t: return
        key = (from_t, from_c, to_t, to_c)
        if key in seen: return
        seen.add(key)
        relationships.append({
            "fromTable": from_t,
            "fromColumn": from_c,
            "toTable": to_t,
            "toColumn": to_c,
            "relationshipType": "Many-to-One"
        })

    all_elements = root.findall(".//")
    relationship_nodes = [el for el in all_elements if el.tag.endswith("relationship")]
    
    for rel in relationship_nodes:
        expr = rel.find("expression")
        if expr is None: continue
        ops = []
        for sub_expr in expr.iter("expression"):
            op = sub_expr.get("op")
            if op and (op.startswith("[") or op in local_name_map): ops.append(op)
            
        if len(ops) == 2:
            info1 = local_name_map.get(ops[0]) or local_name_map.get(clean(ops[0]))
            info2 = local_name_map.get(ops[1]) or local_name_map.get(clean(ops[1]))
            if info1 and info2:
                add(info1['table'], info1['col'], info2['table'], info2['col'])

    return relationships

# ============================================================
# STEP 4: VISUAL METADATA (FIXED)
# ============================================================
def extract_visual_metadata(root: ET.Element, column_map: Dict[str, List[str]]):
    worksheets_data = []
    dashboards_data = []

    # A. Worksheets
    for worksheet in root.findall(".//worksheet"):
        sheet_name = worksheet.get('name')
        bound_columns_set = set()

        for dep in worksheet.findall(".//datasource-dependencies"):
            for col in dep.findall("column-instance"):
                col_ref = col.get('column')
                clean_col = None
                
                if col_ref and ']:' in col_ref:
                    parts = col_ref.split(']:')
                    if len(parts) > 1: clean_col = clean_visual_column_name(parts[-1])
                
                if not clean_col:
                    clean_col = clean_visual_column_name(col.get('name'))
                    
                if clean_col: bound_columns_set.add(clean_col)

        # NEW: FILTER OUT EMPTY WORKSHEETS
        if not bound_columns_set:
            continue

        visual_type = "Automatic"
        for mark_element in worksheet.findall(".//pane/mark"):
            cls = mark_element.get('class')
            if cls and cls != "Automatic":
                visual_type = MARK_MAP.get(cls.lower(), cls.capitalize())
                break
        
        if visual_type == "Automatic":
            if worksheet.find(".//style-rule[@element='map']") is not None: visual_type = "Map"
            elif worksheet.find(".//style-rule[@element='table']") is not None: visual_type = "Text Table"
            else:
                col_list_lower = [c.lower() for c in bound_columns_set]
                map_keywords = ['lat', 'lon', 'country', 'city', 'state', 'zip', 'geo']
                if any(k in col for col in col_list_lower for k in map_keywords): visual_type = "Map"
                elif len(bound_columns_set) == 1: visual_type = "Text Table"
                else: visual_type = "Bar Chart"

        formatted_columns = []
        for col in sorted(list(bound_columns_set)):
            possible_tables = column_map.get(col, ["Unknown"])
            table_name = possible_tables[0] if possible_tables else "Unknown"
            formatted_columns.append({"table": table_name, "column": col})

        worksheets_data.append({
            "name": sheet_name,
            "visualType": visual_type,
            "columns": formatted_columns
        })

    # B. Dashboards
    for dashboard in root.findall(".//dashboard"):
        ws_names = []
        for zone in dashboard.findall(".//zone"):
            z_name = zone.get("name")
            if z_name: ws_names.append(z_name)
        
        dashboards_data.append({
            "dashboardName": dashboard.get("name"),
            "worksheets": list(set(ws_names))
        })

    return worksheets_data, dashboards_data

# ============================================================
# MAIN ENTRY POINT
# ============================================================
def extract_metadata_from_twbx(twbx_path: str):
    # This function remains unchanged, utilizing the updated logic above
    log.info(f"TWBX SIZE: {os.path.getsize(twbx_path)} bytes")
    
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(twbx_path, "r") as z: z.extractall(tmp)

        twb = hyper = None
        for root_dir, _, files in os.walk(tmp):
            for f in files:
                if f.endswith(".twb"): twb = os.path.join(root_dir, f)
                elif f.endswith(".hyper"): hyper = os.path.join(root_dir, f)

        if not twb: raise ValueError("Invalid TWBX: No .twb found")

        tree = ET.parse(twb)
        root = tree.getroot()
        strip_ns(root)

        xml_tables, local_name_map = extract_xml_metadata(root)
        hyper_tables = {}
        if hyper: hyper_tables = extract_hyper_metadata(hyper)
        
        final_tables = {k: v for k, v in xml_tables.items() if not is_junk_table(k)}
        for tbl, cols in hyper_tables.items():
            if not is_junk_table(tbl) and tbl not in final_tables:
                final_tables[tbl] = cols

        relationships = extract_relationships(root, final_tables, local_name_map)

        column_map = {}
        for tbl, cols in final_tables.items():
            for c in cols: column_map.setdefault(c, []).append(tbl)

        worksheets, dashboards = extract_visual_metadata(root, column_map)

        return {
            "tables": final_tables,
            "relationships": relationships,
            "worksheets": worksheets,
            "dashboards": dashboards
        }

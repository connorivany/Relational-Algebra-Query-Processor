def parseTable(inputStr):
    #1 - Split int tableName (columns...) and rows
    header, rowsStr = inputStr.split("=", 1)
    header = header.strip()
    rowsStr = rowsStr.strip()

    #2 - Extract table name and column names
    tableName, columnsStr = header.split("(", 1)
    tableName = tableName.strip()
    columns = [c.strip() for c in columnsStr[:-1].split(",")] # remove')'

    #3 - Remove curly braces and split into lines
    rowsStr = rowsStr.lstrip("{").rstrip("}")
    rows = []
    for line in rowsStr.splitlines():
        line = line.strip()
        if not line:
            continue
        values = [v.strip() for v in line.split(",")]
        row = dict(zip(columns, values))
        rows.append(row)

    return tableName, rows

#Selection (condition)
def select(table, conditionFN):
    return [row for row in table if conditionFN(row)]

#Projection (pick columns)
def project(table, columns):
    return [{col: row[col] for col in columns} for row in table]

#Join (natural join on matching keys
def join(table1, table2, key1, key2):
    result = []
    for r1 in table1:
        for r2 in table2:
            if r1[key1] == r2[key2]:
                merged = {**r1, **r2}
                result.append(merged)
    return result

#Union, Intersection, Difference
def union(table1, table2):
    return [dict(t) for t in {tuple(sorted(row.items())) for row in (table1 + table2)}]

def intersection(table1,table2):
    set1 = {tuple(sorted(row.items())) for row in table1}
    set2 = {tuple(sorted(row.items())) for row in table2}
    return [dict(t) for t in set1 & set2]

def difference(table1, table2):
    set1 = {tuple(sorted(row.items())) for row in table1}
    set2 = {tuple(sorted(row.items())) for row in table2}
    return [dict(t) for t in set1 - set2]

def evaluate(query, db):
    query = query.strip()

    # Base case: it's just a table name
    if query in db:
        return db[query]

    # Selection
    if query.startswith("σ"):
        cond, rest = query[1:].split("(", 1)
        cond = cond.strip()
        inner = rest[:-1].strip()  # contents inside ()
        rows = evaluate(inner, db)  # RECURSE

        # parse condition
        if ">" in cond:
            col, val = cond.split(">", 1)
            fn = lambda r: int(r[col.strip()]) > int(val.strip())
        elif "<" in cond:
            col, val = cond.split("<", 1)
            fn = lambda r: int(r[col.strip()]) < int(val.strip())
        elif "=" in cond:
            col, val = cond.split("=", 1)
            fn = lambda r: r[col.strip()] == val.strip()
        else:
            raise ValueError("Unsupported condition")

        return select(rows, fn)

    # Projection
    if query.startswith("π"):
        cols, rest = query[1:].split("(", 1)
        cols = [c.strip() for c in cols.strip().split(",")]
        inner = rest[:-1].strip()
        rows = evaluate(inner, db)  # RECURSE
        return project(rows, cols)

    # Join
    if "⋈" in query:
        # Expect format: Table1 ⋈ Table1.Key=Table2.Key Table2
        left_part, right_part = query.split("⋈", 1)
        left_part = left_part.strip()
        # parse join condition and right table
        cond_and_right = right_part.strip().split(None, 1)
        if len(cond_and_right) != 2:
            raise ValueError("Join syntax must be: Table1 ⋈ Table1.Key=Table2.Key Table2")
        cond, right_table_name = cond_and_right
        right_table_name = right_table_name.strip()

        if "=" not in cond:
            raise ValueError("Join condition must be equality like Table1.Key=Table2.Key")
        key1_str, key2_str = cond.split("=")
        key1_table, key1 = key1_str.split(".")
        key2_table, key2 = key2_str.split(".")
        key1 = key1.strip()
        key2 = key2.strip()

        left_rows = evaluate(left_part, db)
        right_rows = evaluate(right_table_name, db)
        return join(left_rows, right_rows, key1, key2)

    # Set operations: ∪ (union), ∩ (intersection), − (difference)
    for op_symbol, op_fn in [("∪", union), ("∩", intersection), ("−", difference)]:
        if op_symbol in query:
            parts = query.split(op_symbol)
            if len(parts) != 2:
                raise ValueError(f"Set operation {op_symbol} requires exactly two tables/queries")
            left = evaluate(parts[0].strip(), db)
            right = evaluate(parts[1].strip(), db)
            return op_fn(left, right)

    raise ValueError("Unsupported query: " + query)





def main():
    db = {}

    while True:
        print("\nOptions:")
        print("1. Add new table")
        print("2. Run query")
        print("3. Exit")

        choice = input("Choose an option: ").strip()

        if choice == "1":
            print("Enter a table definition (end with a blank line):")
            lines = []
            while True:
                line = input()
                if not line.strip():
                    break
                lines.append(line)
            user_input = "\n".join(lines)
            table, rows = parseTable(user_input)
            db[table] = rows
            print(f"Added table '{table}' with {len(rows)} rows.")

        elif choice == "2":
            print("Enter query (examples: σ Age>28 (Employees), π Name,Age (Employees))")
            query = input("Query: ").strip()

            try:
                result = evaluate(query, db)  # delegate to recursive evaluator
                print("Result:", result)

            except Exception as e:
                print("❌ Error:", e)

if __name__ == "__main__":
    main()
#include <iostream>
#include <unordered_map>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <windows.h>
#include <iomanip>
#include <chrono>

using namespace std;

vector<unordered_map<string, string>> currentTable;
string currentFile = "";
string dbFolder = "C:\\database\\";



string walFile() { return currentFile + ".wal"; }
string lockFile() { return currentFile + ".lock"; }
string tmpFile()  { return currentFile + ".tmp"; }

HANDLE lockHandle = INVALID_HANDLE_VALUE;

bool acquireLock() {
    lockHandle = CreateFileA(
        lockFile().c_str(),
        GENERIC_WRITE, 0,          
        NULL, CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE, NULL
    );
    if (lockHandle == INVALID_HANDLE_VALUE) {
        cout << "[ACID] Could not acquire lock. Another process may be using this database.\n";
        return false;
    }
    return true;
}

void releaseLock() {
    if (lockHandle != INVALID_HANDLE_VALUE) {
        CloseHandle(lockHandle);
        lockHandle = INVALID_HANDLE_VALUE;
    }
}

void writeWAL(const string& operation) {
    ofstream wal(walFile(), ios::app);
    if (wal) wal << operation << "\n";
}

void clearWAL() {
    DeleteFileA(walFile().c_str());
}

bool walExists() {
    return GetFileAttributesA(walFile().c_str()) != INVALID_FILE_ATTRIBUTES;
}


void printBanner() {
    cout << "#################################\n";
    cout << "#        welcome to soldb       #\n";
    cout << "#################################\n";
}

bool checkFileSelected() {
    if (currentFile.empty()) {
        cout << "No database selected. Use: use <dbname> or create <dbname>\n";
        return false;
    }
    return true;
}

void loadFromFile() {
    currentTable.clear();
    ifstream in(currentFile);
    if (!in) return;
    string line;
    while (getline(in, line)) {
        if (line.empty()) continue;
        unordered_map<string, string> row;
        stringstream ss(line);
        string field;
        while (getline(ss, field, ',')) {
            size_t pos = field.find('=');
            if (pos != string::npos)
                row[field.substr(0, pos)] = field.substr(pos + 1);
        }
        if (!row.empty()) currentTable.push_back(row);
    }
    in.close();
}


bool saveFileAtomic() {
    if (!checkFileSelected()) return false;

    string tmp = tmpFile();
    HANDLE hFile = CreateFileA(
        tmp.c_str(), GENERIC_WRITE, 0, NULL,
        CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL
    );
    if (hFile == INVALID_HANDLE_VALUE) {
        cout << "[ACID] Failed to open temp file for writing.\n";
        return false;
    }

  
    string content;
    for (auto& row : currentTable) {
        vector<pair<string,string>> sorted(row.begin(), row.end());
        sort(sorted.begin(), sorted.end());
        bool first = true;
        for (auto& kv : sorted) {
            if (!first) content += ",";
            content += kv.first + "=" + kv.second;
            first = false;
        }
        content += "\n";
    }

    DWORD written;
    WriteFile(hFile, content.c_str(), (DWORD)content.size(), &written, NULL);

    FlushFileBuffers(hFile);
    CloseHandle(hFile);

    
    if (!MoveFileExA(tmp.c_str(), currentFile.c_str(), MOVEFILE_REPLACE_EXISTING)) {
        cout << "[ACID] Atomic rename failed. Data is safe in " << tmp << "\n";
        return false;
    }

    clearWAL();  
    cout << "Saved to " << currentFile << "\n";
    return true;
}


bool validateRow(const unordered_map<string,string>& row, int rowNum = -1) {
    for (auto& kv : row) {
        if (kv.first.empty()) {
            cout << "[ACID] Consistency error: empty key in row " << rowNum << ". Rejected.\n";
            return false;
        }
        if (kv.first.size() > 64) {
            cout << "[ACID] Consistency error: key '" << kv.first << "' exceeds 64 chars. Rejected.\n";
            return false;
        }
        if (kv.second.size() > 1024) {
            cout << "[ACID] Consistency error: value for '" << kv.first << "' exceeds 1024 chars. Rejected.\n";
            return false;
        }
    }
    return true;
}


void recoverWAL() {
    if (!walExists()) return;
    cout << "[ACID] Warning: incomplete write detected (crash recovery).\n";
    cout << "[ACID] Discarding uncommitted WAL. Database restored to last good state.\n";
    clearWAL();
}


bool inTransaction = false;
vector<unordered_map<string, string>> transactionSnapshot;

void beginTransaction() {
    if (inTransaction) { cout << "Already in a transaction. COMMIT or ROLLBACK first.\n"; return; }
    loadFromFile();
    transactionSnapshot = currentTable;
    inTransaction = true;
    writeWAL("BEGIN");
    cout << "[ACID] Transaction started. Changes are pending until COMMIT.\n";
}

void commitTransaction() {
    if (!inTransaction) { cout << "No active transaction.\n"; return; }
    writeWAL("COMMIT");
    if (saveFileAtomic()) {
        inTransaction = false;
        transactionSnapshot.clear();
        cout << "[ACID] Transaction committed.\n";
    }
}

void rollbackTransaction() {
    if (!inTransaction) { cout << "No active transaction.\n"; return; }
    currentTable = transactionSnapshot;
    transactionSnapshot.clear();
    inTransaction = false;
    clearWAL();
    cout << "[ACID] Transaction rolled back. All changes discarded.\n";
}

// ─── Save wrapper: in a transaction, defer disk write until COMMIT ────────────
void saveFile() {
    if (inTransaction) {
        cout << "[ACID] Change staged (not yet committed). Use COMMIT to persist or ROLLBACK to undo.\n";
        return;
    }
    saveFileAtomic();
}

// ─── Remaining commands (unchanged logic, ACID-aware save) ───────────────────
void createFile(const string& filename) {
    currentFile = dbFolder + filename + ".db";
    currentTable.clear();
    recoverWAL();
    HANDLE h = CreateFileA(currentFile.c_str(), GENERIC_WRITE, 0, NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE && GetLastError() != ERROR_FILE_EXISTS) {
        cout << "Failed to create database.\n"; return;
    }
    if (h != INVALID_HANDLE_VALUE) CloseHandle(h);
    cout << "Database '" << filename << ".db' created and now in use.\n";
}

void useFile(const string& filename) {
    currentFile = dbFolder + filename + ".db";
    recoverWAL();
    loadFromFile();
    cout << "Using file '" << filename << ".db'\n";
}

string normalizeInput(const string& input) {
    string result;
    for (size_t i = 0; i < input.size(); i++) {
        if (input[i] == '=') {
            while (!result.empty() && result.back() == ' ') result.pop_back();
            result += '=';
            i++;
            while (i < input.size() && input[i] == ' ') i++;
            i--;
        } else {
            result += input[i];
        }
    }
    return result;
}

unordered_map<string,string> parseFields(const string& input) {
    unordered_map<string,string> rec;
    stringstream kvs(normalizeInput(input));
    string token;
    while (kvs >> token) {
        size_t pos = token.find('=');
        if (pos != string::npos)
            rec[token.substr(0, pos)] = token.substr(pos + 1);
    }
    return rec;
}

void insertIntoTable(const string& input) {
    if (!checkFileSelected()) return;
    if (!inTransaction) loadFromFile();
    stringstream rs(normalizeInput(input));
    string recordToken;
    int count = 0;
    while (getline(rs, recordToken, '|')) {
        auto rec = parseFields(recordToken);
        if (rec.empty()) continue;
        // ─── ACID: Consistency check before inserting ─────────────────────
        if (!validateRow(rec, (int)currentTable.size() + 1)) continue;
        writeWAL("INSERT " + recordToken);
        currentTable.push_back(rec);
        count++;
    }
    cout << "Inserted " << count << " record(s). Total rows: " << currentTable.size() << "\n";
    saveFile();
}

void showTable() {
    if (!checkFileSelected()) return;
    if (!inTransaction) loadFromFile();
    if (currentTable.empty()) { cout << "Table is empty.\n"; return; }

    vector<string> cols;
    for (auto& row : currentTable)
        for (auto& kv : row)
            if (find(cols.begin(), cols.end(), kv.first) == cols.end())
                cols.push_back(kv.first);
    sort(cols.begin(), cols.end());

    size_t idWidth = 2;
    vector<size_t> widths(cols.size());
    for (size_t i = 0; i < cols.size(); i++) widths[i] = cols[i].size();
    for (auto& row : currentTable)
        for (size_t i = 0; i < cols.size(); i++) {
            auto it = row.find(cols[i]);
            if (it != row.end()) widths[i] = max(widths[i], it->second.size());
        }

    string sep = "+" + string(idWidth + 2, '-');
    for (size_t i = 0; i < cols.size(); i++) sep += "+" + string(widths[i] + 2, '-');
    sep += "+";

    cout << sep << "\n";
    cout << "| " << setw(idWidth) << left << "ID" << " ";
    for (size_t i = 0; i < cols.size(); i++)
        cout << "| " << setw(widths[i]) << left << cols[i] << " ";
    cout << "|\n" << sep << "\n";

    for (size_t r = 0; r < currentTable.size(); r++) {
        cout << "| " << setw(idWidth) << left << (r + 1) << " ";
        for (size_t i = 0; i < cols.size(); i++) {
            auto it = currentTable[r].find(cols[i]);
            string val = (it != currentTable[r].end()) ? it->second : "";
            cout << "| " << setw(widths[i]) << left << val << " ";
        }
        cout << "|\n";
    }
    cout << sep << "\n";
}

void deleteRow(const string& input) {
    if (!checkFileSelected()) return;
    if (!inTransaction) loadFromFile();

    string normalized = normalizeInput(input);
    stringstream ss(normalized);
    string token, idVal;
    vector<string> fieldKeys;

    while (ss >> token) {
        size_t pos = token.find('=');
        if (pos != string::npos) {
            string k = token.substr(0, pos);
            if (k == "id") idVal = token.substr(pos + 1);
            else fieldKeys.push_back(k);
        } else {
            fieldKeys.push_back(token);
        }
    }

    if (idVal.empty()) {
        cout << "Usage: delete id=1  OR  delete id=1 name age\n";
        return;
    }

    int id = stoi(idVal) - 1;
    if (id < 0 || id >= (int)currentTable.size()) { cout << "Row ID not found.\n"; return; }

    writeWAL("DELETE id=" + idVal);
    if (fieldKeys.empty()) {
        currentTable.erase(currentTable.begin() + id);
        cout << "Deleted entire row " << (id + 1) << ".\n";
    } else {
        for (auto& key : fieldKeys) {
            if (currentTable[id].erase(key))
                cout << "Deleted field '" << key << "' from row " << (id + 1) << ".\n";
            else
                cout << "Field '" << key << "' not found.\n";
        }
    }
    saveFile();
}

void updateRow(const string& input) {
    if (!checkFileSelected()) return;
    if (!inTransaction) loadFromFile();
    auto fields = parseFields(input);
    if (!fields.count("id")) { cout << "Specify id=<number>. Example: update id=1 name=samsung\n"; return; }
    int id = stoi(fields["id"]) - 1;
    fields.erase("id");
    if (id < 0 || id >= (int)currentTable.size()) { cout << "Row ID not found.\n"; return; }

    
    auto merged = currentTable[id];
    for (auto& kv : fields) merged[kv.first] = kv.second;
    if (!validateRow(merged, id + 1)) return;

    writeWAL("UPDATE " + input);
    for (auto& kv : fields) {
        currentTable[id][kv.first] = kv.second;
        cout << "Updated row " << (id + 1) << ": " << kv.first << " = " << kv.second << "\n";
    }
    saveFile();
}

void readRow(const string& input) {
    if (!checkFileSelected()) return;
    if (!inTransaction) loadFromFile();
    auto fields = parseFields(input);
    if (fields.count("id")) {
        int id = stoi(fields["id"]) - 1;
        if (id < 0 || id >= (int)currentTable.size()) { cout << "Row not found.\n"; return; }
        for (auto& kv : currentTable[id]) cout << kv.first << " = " << kv.second << "\n";
        return;
    }
    for (size_t r = 0; r < currentTable.size(); r++) {
        bool match = true;
        for (auto& kv : fields) {
            auto it = currentTable[r].find(kv.first);
            if (it == currentTable[r].end() || it->second != kv.second) { match = false; break; }
        }
        if (match) {
            cout << "Row " << (r + 1) << ": ";
            for (auto& kv : currentTable[r]) cout << kv.first << "=" << kv.second << " ";
            cout << "\n";
        }
    }
}

void truncateTable() {
    if (!checkFileSelected()) return;
    cout << "Are you sure? (yes/no): ";
    string confirm; getline(cin, confirm);
    if (confirm != "yes") { cout << "Cancelled.\n"; return; }
    writeWAL("TRUNCATE");
    currentTable.clear();
    saveFile();
    cout << "Table truncated.\n";
}

void printHelp() {
    cout << "\nCommands:\n";
    cout << "  create  / cr  <name>                  - create new database\n";
    cout << "  use     / u   <name>                  - open existing database\n";
    cout << "  insert  / i   name=oppo age=1         - insert a new row\n";
    cout << "  insert  / i   name=a | name=b age=2   - insert multiple rows\n";
    cout << "  show    / sh                          - display all rows as table\n";
    cout << "  read    / r                           - show all rows\n";
    cout << "  read    / r   name=oppo               - filter rows by field value\n";
    cout << "  read    / r   id=1                    - show specific row\n";
    cout << "  update  / up  id=1 name=samsung       - update fields in a row\n";
    cout << "  delete  / del id=1                    - delete entire row\n";
    cout << "  delete  / del id=1 name age           - delete only these fields\n";
    cout << "  truncate/ tr                          - clear all rows\n";
    cout << "  begin   / b                           - start a transaction\n";
    cout << "  commit  / c                           - commit transaction to disk\n";
    cout << "  rollback/ rb                          - undo all changes in transaction\n";
    cout << "  help    / h                           - show this help\n";
    cout << "  exit    / q                           - quit\n\n";

}

void startShell() {
    printBanner();
    string line;
    while (true) {
        if (inTransaction)
            cout << "soldb(txn)> ";
        else
            cout << "soldb> ";

        getline(cin, line);
        if (line.empty()) continue;
        stringstream ss(line);
        string command;
        ss >> command;
        string rest;
        getline(ss, rest);
        if (!rest.empty() && rest[0] == ' ') rest = rest.substr(1);

        if (command == "exit" || command == "q") {
            if (inTransaction) {
                cout << "You have an open transaction. COMMIT or ROLLBACK before exiting.\n";
                continue;
            }
            break;
        }

        auto start = chrono::high_resolution_clock::now();

        if      (command == "help"     || command == "h")   printHelp();
        else if (command == "create"   || command == "cr")  { string f; stringstream(rest) >> f; createFile(f); }
        else if (command == "use"      || command == "u")   { string f; stringstream(rest) >> f; useFile(f); }
        else if (command == "show"     || command == "sh")  showTable();
        else if (command == "insert"   || command == "i")   insertIntoTable(rest);
        else if (command == "read"     || command == "r")   readRow(rest);
        else if (command == "update"   || command == "up")  updateRow(rest);
        else if (command == "delete"   || command == "del") deleteRow(rest);
        else if (command == "truncate" || command == "tr")  truncateTable();
        else if (command == "begin"    || command == "b")   beginTransaction();
        else if (command == "commit"   || command == "c")   commitTransaction();
        else if (command == "rollback" || command == "rb")  rollbackTransaction();
        else { cout << "Unknown command. Type 'help' or 'h' for commands.\n"; continue; }

        auto end = chrono::high_resolution_clock::now();
        double ms = chrono::duration<double, milli>(end - start).count();
        if (ms < 1.0)
            cout << "(" << fixed << setprecision(3) << ms * 1000 << " us)\n";
        else
            cout << "(" << fixed << setprecision(3) << ms << " ms)\n";
    }
}

int main() {
    SetConsoleTitleA("soldb");
    CreateDirectoryA("C:\\database", NULL);
    startShell();
    return 0;
}
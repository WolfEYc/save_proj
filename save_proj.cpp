#include <iostream>
#include <sstream>
#include <unordered_map>
#include <fstream>
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <typeinfo>
#include <Windows.h>
using namespace std;

const string Rule1QueryFile = "Rule1Query.sql";
const string Rule2QueryFile = "Rule2Query.sql";

const string rule_1_header = "last_name,first_name,account_number,purchase_number,merchant_name,purchase_amount";
struct Rule1Row {
    string last_name;
    string first_name;
    int account_number;
    int purchase_number;
    string merchant_name;
    double purchase_amount;
    
    Rule1Row(sql::ResultSet* res) {
        last_name = res->getString("last_name");
        first_name = res->getString("first_name");
        account_number = res->getInt("account_number");
        purchase_number = res->getInt("purchase_number");
        merchant_name = res->getString("merchant_name");
        purchase_amount = res->getDouble("purchase_amount");
        //purchase_amount = abs(purchase_amount);
    }

    string hash() {
        return merchant_name + to_string(account_number);
    }

    string csv() {
        stringstream ss;
        ss << last_name << ','
            << first_name << ','
            << account_number << ','
            << purchase_number << ','
            << merchant_name << ','
            << purchase_amount;
        return ss.str();
    }
};

string str_from_file(string filename) {
    ifstream file(filename);
    std::string str((std::istreambuf_iterator<char>(file)),
        std::istreambuf_iterator<char>());
    return str;
}

const double iqr_mult = 1.5;
const double bound_padding = 100;

struct FraudBound {
    double lower;
    double upper;
};

FraudBound fraud_bound(const vector<Rule1Row>& purchases) {
    size_t s = purchases.size();
    double q1 = purchases[s / 4].purchase_amount;
    double q3 = purchases[3 * s / 4].purchase_amount;

    // Calculate IQR
    double iqr = q3 - q1;

    FraudBound fb;

    fb.lower = (q1 - iqr_mult * iqr) - bound_padding;
    fb.upper = (q3 + iqr_mult * iqr) + bound_padding;
    
    return fb;
}

const size_t ReqPurchasesToFlag = 4;

void add_flagged(const vector<Rule1Row>& purchases, vector<Rule1Row>& flagged_purchases, const FraudBound fb) {
    for (Rule1Row row : purchases) {
        if (row.purchase_amount > fb.lower && row.purchase_amount < fb.upper) continue;
        flagged_purchases.push_back(row);
    }
}

void run_rule_1(sql::Statement* stmt) {
    string query = str_from_file(Rule1QueryFile);
    sql::ResultSet* res = stmt->executeQuery(query);
   
    unordered_map<string, vector<Rule1Row>> recurring_purchases;

    while (res->next()) {
        Rule1Row row(res);
        recurring_purchases[row.hash()].push_back(row);
    }

    vector<Rule1Row> flagged_purchases;

    for (const auto& recurring_purchase : recurring_purchases) {
        const vector<Rule1Row>& purchases = recurring_purchase.second;
        if (purchases.size() < ReqPurchasesToFlag) continue;
        FraudBound bound = fraud_bound(purchases);
        add_flagged(purchases, flagged_purchases, bound);
    }

    cout << rule_1_header << endl;
    for (Rule1Row row : flagged_purchases) {
        cout << row.csv() << endl;
    }

    delete res;
}

const string rule_2_header = "last_name,first_name,account_number,purchase_number,account_state,merchant_state";
struct Rule2Row {
    string last_name;
    string first_name;
    int account_number;
    int purchase_number;
    string account_state;
    string merchant_state;

    Rule2Row(sql::ResultSet* res) {
        last_name = res->getString("last_name");
        first_name = res->getString("first_name");
        account_number = res->getInt("account_number");
        purchase_number = res->getInt("purchase_number");
        account_state = res->getString("account_state");
        merchant_state = res->getString("merchant_state");
    }

    string csv() {
        stringstream ss;
        ss << last_name << ','
            << first_name << ','
            << account_number << ','
            << purchase_number << ','
            << account_state << ','
            << merchant_state;
        return ss.str();
    }
};

void run_rule_2(sql::Statement* stmt) {
    string query = str_from_file(Rule2QueryFile);
    sql::ResultSet* res = stmt->executeQuery(query);

    cout << rule_2_header << endl;
    while (res->next()) {
        Rule2Row row(res);
        cout << row.csv() << endl;
    }

    delete res;
}

int main()
{
    string url;
    string username;
    string password;
    string db;
    int ruleToRun;

    cin >> url
        >> username
        >> password
        >> db
        >> ruleToRun;

    sql::Driver* driver;
    sql::Connection* con;
    sql::Statement* stmt;

    driver = sql::mysql::get_mysql_driver_instance();
    con = driver->connect(url, username, password);
    con->setSchema(db);
    stmt = con->createStatement();

    switch (ruleToRun) {
        case 1:
            run_rule_1(stmt);
            break;
        case 2:
            run_rule_2(stmt);
            break;
    }

    delete stmt;
    delete con;
}


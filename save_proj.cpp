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
using namespace std;

const string Rule1QueryFile = "Rule1Query.sql";
const string Rule2QueryFile = "Rule2Query.sql";

const size_t ReqPurchasesToFlag = 4;

sql::Driver* driver;
sql::Connection* con;
sql::Statement* stmt;
sql::ResultSet* res;

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
    }

    string hash() {
        return merchant_name + to_string(account_number);
    }

    stringbuf* csv() {
        stringstream ss;
        ss << last_name << ','
            << first_name << ','
            << account_number << ','
            << purchase_number << ','
            << merchant_name << ','
            << purchase_amount;

        return ss.rdbuf();
    }
};

string str_from_file(string filename) {
    ifstream file(filename);
    std::string str((std::istreambuf_iterator<char>(file)),
        std::istreambuf_iterator<char>());
    return str;
}

void add_flagged(const vector<Rule1Row>& purchases, vector<Rule1Row>& flagged_purchases, const double bound) {
    for (Rule1Row row : purchases) {
        if (row.purchase_amount < bound) continue;
        flagged_purchases.push_back(row);
    }
}

double fraud_bound(const vector<Rule1Row>& purchases) {
    size_t s = purchases.size();
    double q1 = purchases[s / 4].purchase_amount;
    double q3 = purchases[3 * s / 4].purchase_amount;

    // Calculate IQR
    double iqr = q3 - q1;

    return q3 + 1.5 * iqr;
}

void run_rule_1() {
    stmt->clearAttributes();
    stmt->clearWarnings();
    string query1 = str_from_file(Rule1QueryFile);
    res = stmt->executeQuery(query1);
   
    unordered_map<string, vector<Rule1Row>> recurring_purchases;

    while (res->next()) {
        Rule1Row row(res);
        recurring_purchases[row.hash()].push_back(row);
    }

    vector<Rule1Row> flagged_purchases;

    for (const auto& recurring_purchase : recurring_purchases) {
        const vector<Rule1Row>& purchases = recurring_purchase.second;
        if (purchases.size() < ReqPurchasesToFlag) continue;
        double bound = fraud_bound(purchases);
        add_flagged(purchases, flagged_purchases, bound);
    }

    cout << rule_1_header << endl;
    for (Rule1Row row : flagged_purchases) {
        cout << row.csv() << endl;
    }
}

void run_rule_2() {
    stmt->clearAttributes();
    stmt->clearWarnings();


}

int main()
{
    string url;
    string username;
    string password;

    cin >> url >> username >> password;

    driver = sql::mysql::get_mysql_driver_instance();
    con = driver->connect(url, username, password);

    stmt = con->createStatement();
   
    run_rule_1();
    run_rule_2();

    delete res;
    delete stmt;
    delete con;
}


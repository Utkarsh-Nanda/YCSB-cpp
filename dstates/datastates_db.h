//
//  datastates_db.h
//  YCSB-cpp
//  Copyright (c) 2022 Bogdan Nicolae <bogdan.nicolae@acm.org>
//

#ifndef YCSB_C_DATASTATES_DB_H_
#define YCSB_C_DATASTATES_DB_H_

#include <string>
#include <mutex>
#include <atomic>
#include "core/db.h"
#include "utils/properties.h"
#include <vector>
#include <limits>
#include <unordered_map>
#include "dstates/vordered_kv.hpp"

#define __DEBUG
#include "dstates/debug.hpp"

namespace ycsbc {

//typedef vordered_kv_t<std::string, std::string> dstates_kv_t;
 typedef vordered_kv_t<std::string, std::string, emem_history_t<std::string, std::string>> dstates_kv_t;

class DataStatesDB : public DB {
public:
    DataStatesDB() {}
    ~DataStatesDB() {}


    void Init();
    void Cleanup();



    
    static void PrintStatistics();  
    static  void UpdateStatistics(const std::string &key, bool is_insert);
   




    int count = 0;
    Status Read(const std::string &table, const std::string &key,
                const std::vector<std::string> *fields, std::vector<Field> &result) {
        return (this->*(method_read_))(table, key, fields, result);
    }

    Status Scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
        return (this->*(method_scan_))(table, key, len, fields, result);
    }

    Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
        return (this->*(method_update_))(table, key, values);
    }

    Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
        return (this->*(method_insert_))(table, key, values);
    }

    Status Delete(const std::string &table, const std::string &key) {
        return (this->*(method_delete_))(table, key);
    }

private:


    static std::atomic<int> insert_count;
    static std::atomic<int> update_count;
    static std::unordered_map<std::string, int> operations_per_key;
    static std::atomic<int> min_ops_per_key;
    static std::atomic<int> max_ops_per_key;
    static std::atomic<int> total_ops;
    static std::atomic<int> unique_keys;






    static void SerializeRow(const std::vector<Field> &values, std::string &data);
    static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields);
    static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields);
    static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
    static void DeserializeRow(std::vector<Field> &values, const std::string &data);

    Status ReadSingle(const std::string &table, const std::string &key,
                      const std::vector<std::string> *fields, std::vector<Field> &result);
    Status ScanSingle(const std::string &table, const std::string &key, int len,
                      const std::vector<std::string> *fields,
                      std::vector<std::vector<Field>> &result);
    Status UpdateSingle(const std::string &table, const std::string &key,
                        std::vector<Field> &values);
    Status MergeSingle(const std::string &table, const std::string &key,
                       std::vector<Field> &values);
    Status InsertSingle(const std::string &table, const std::string &key,
                        std::vector<Field> &values);
    Status DeleteSingle(const std::string &table, const std::string &key);

    Status (DataStatesDB::*method_read_)(const std::string &, const std:: string &,
                                      const std::vector<std::string> *, std::vector<Field> &);
    Status (DataStatesDB::*method_scan_)(const std::string &, const std::string &,
                                      int, const std::vector<std::string> *,
                                      std::vector<std::vector<Field>> &);
    Status (DataStatesDB::*method_update_)(const std::string &, const std::string &,
                                        std::vector<Field> &);
    Status (DataStatesDB::*method_insert_)(const std::string &, const std::string &,
                                        std::vector<Field> &);
    Status (DataStatesDB::*method_delete_)(const std::string &, const std::string &);

    int fieldcount_;

    static dstates_kv_t *db_;
    static int ref_cnt_;
    static std::mutex mu_;
};

DB *NewDataStatesDB();

} // ycsbc

#endif // YCSB_C_DATASTATES_DB_H_

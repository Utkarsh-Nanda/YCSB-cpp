//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2022 Bogdan Nicolae <bogdan.nicolae@acm.org>
//

#include "datastates_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"

namespace {
  const std::string PROP_NAME = "datastates.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_MERGEUPDATE = "datastates.mergeupdate";
  const std::string PROP_MERGEUPDATE_DEFAULT = "false";

  const std::string PROP_DESTROY = "datastates.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";
} // anonymous

namespace ycsbc {




std::atomic<int> DataStatesDB::insert_count(0);
std::atomic<int> DataStatesDB::update_count(0);
std::unordered_map<std::string, int> DataStatesDB::operations_per_key;
std::atomic<int> DataStatesDB::min_ops_per_key(std::numeric_limits<int>::max());
std::atomic<int> DataStatesDB::max_ops_per_key(0);
std::atomic<int> DataStatesDB::total_ops(0);
std::atomic<int> DataStatesDB::unique_keys(0);



dstates_kv_t *DataStatesDB::db_ = nullptr;
int DataStatesDB::ref_cnt_ = 0;
std::mutex DataStatesDB::mu_;


void DataStatesDB::Init() {
    const std::lock_guard<std::mutex> lock(mu_);

    const utils::Properties &props = *props_;
    method_read_ = &DataStatesDB::ReadSingle;
    method_scan_ = &DataStatesDB::ScanSingle;
    method_update_ = &DataStatesDB::UpdateSingle;
    method_insert_ = &DataStatesDB::InsertSingle;
    method_delete_ = &DataStatesDB::DeleteSingle;
#ifdef USE_MERGEUPDATE
    if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true")
	throw utils::Exception("DataStates merge updates not yet implemented");
#endif
    fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
					      CoreWorkload::FIELD_COUNT_DEFAULT));

    ref_cnt_++;
    if (db_)
	return;

    const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
    if (db_path == "")
	throw utils::Exception("DataStates db path is missing");

    if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
	DBG("deleting " << db_path);
	unlink(db_path.c_str());
    }
    db_ = new dstates_kv_t(db_path);
}

void DataStatesDB::Cleanup() {
    const std::lock_guard<std::mutex> lock(mu_);
    if (--ref_cnt_)
	return;
    delete db_;
}

void DataStatesDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
    for (const Field &field : values) {
	uint32_t len = field.name.size();
	data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
	data.append(field.name.data(), field.name.size());
	len = field.value.size();
	data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
	data.append(field.value.data(), field.value.size());
    }
}

void DataStatesDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
					const std::vector<std::string> &fields) {
    std::vector<std::string>::const_iterator filter_iter = fields.begin();
    while (p != lim && filter_iter != fields.end()) {
	assert(p < lim);
	uint32_t len = *reinterpret_cast<const uint32_t *>(p);
	p += sizeof(uint32_t);
	std::string field(p, static_cast<const size_t>(len));
	p += len;
	len = *reinterpret_cast<const uint32_t *>(p);
	p += sizeof(uint32_t);
	std::string value(p, static_cast<const size_t>(len));
	p += len;
	if (*filter_iter == field) {
	    values.push_back({field, value});
	    filter_iter++;
	}
    }
    assert(values.size() == fields.size());
}

void DataStatesDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
					const std::vector<std::string> &fields) {
    const char *p = data.data();
    const char *lim = p + data.size();
    DeserializeRowFilter(values, p, lim, fields);
}

void DataStatesDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
    while (p != lim) {
	assert(p < lim);
	uint32_t len = *reinterpret_cast<const uint32_t *>(p);
	p += sizeof(uint32_t);
	std::string field(p, static_cast<const size_t>(len));
	p += len;
	len = *reinterpret_cast<const uint32_t *>(p);
	p += sizeof(uint32_t);
	std::string value(p, static_cast<const size_t>(len));
	p += len;
	values.push_back({field, value});
    }
}

void DataStatesDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
    const char *p = data.data();
    const char *lim = p + data.size();
    DeserializeRow(values, p, lim);
}

DB::Status DataStatesDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
    std::string data = db_->find(std::numeric_limits<int>::max(), key);
    if (data == dstates_kv_t::low_marker)
	return kNotFound;
    if (fields != nullptr)
	DeserializeRowFilter(result, data, *fields);
    else {
	DeserializeRow(result, data);
	assert(result.size() == static_cast<size_t>(fieldcount_));
    }
    return kOK;
}

DB::Status DataStatesDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
    throw utils::Exception("vordered_kv_t: scannning over multiple values for the same key not supported");
    return kOK;
}

DB::Status DataStatesDB::UpdateSingle(const std::string &table, const std::string &key,
				      std::vector<Field> &values) {
    std::string data = db_->find(std::numeric_limits<int>::max(), key);
    if (data == dstates_kv_t::low_marker)
	return kNotFound;
    std::vector<Field> current_values;
    DeserializeRow(current_values, data);
    assert(current_values.size() == static_cast<size_t>(fieldcount_));
    for (Field &new_field : values) {
	bool found __attribute__((unused)) = false;
	for (Field &cur_field : current_values) {
	    if (cur_field.name == new_field.name) {
		found = true;
		cur_field.value = new_field.value;
		break;
	    }
	}
	assert(found);
    }
    data.clear();
    SerializeRow(current_values, data);
    if (!db_->insert(key, data))
	throw utils::Exception("vordered_kv_t::update failed");
    
    
    UpdateStatistics(key, false);

    
    return kOK;
}

DB::Status DataStatesDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
    throw utils::Exception("vordered_kv_t: merge not implemented");
    return kOK;
}

DB::Status DataStatesDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  if (!db_->insert(key, data))
      throw utils::Exception("vordered_kv_t::insert failed");
  


    UpdateStatistics(key, true);



  return kOK;
}

DB::Status DataStatesDB::DeleteSingle(const std::string &table, const std::string &key) {
    if (!db_->remove(key))
	throw utils::Exception("vordered_kv_t::remove failed");
    return kOK;
}







void DataStatesDB::UpdateStatistics(const std::string& key, bool is_insert) {
    if (is_insert) {
        insert_count++;
    } else {
        update_count++;
    }

    int ops = ++operations_per_key[key];
    if (ops == 1) {
        unique_keys++;
    }

    int current_min = min_ops_per_key.load();
    while (ops < current_min && !min_ops_per_key.compare_exchange_weak(current_min, ops)) {}

    int current_max = max_ops_per_key.load();
    while (ops > current_max && !max_ops_per_key.compare_exchange_weak(current_max, ops)) {}

    total_ops.fetch_add(1, std::memory_order_relaxed);

}

void DataStatesDB::PrintStatistics() {
    std::cout << "Total inserts: " << insert_count << std::endl;
    std::cout << "Total updates: " << update_count << std::endl;
    std::cout << "Unique keys: " << unique_keys << std::endl;
    std::cout << "Min operations per key: " << min_ops_per_key << std::endl;
    std::cout << "Max operations per key: " << max_ops_per_key << std::endl;
    
    int total_unique_keys = unique_keys.load();
    if (total_unique_keys > 0) {
        double avg_ops = (double) total_ops.load() / total_unique_keys;
        std::cout << "Average operations per key: " << avg_ops << std::endl;
    } else {
        std::cout << "Average operations per key: N/A" << std::endl;
    }
}













DB *NewDataStatesDB() {
    return new DataStatesDB;
}

const bool registered = DBFactory::RegisterDB("datastates", NewDataStatesDB);

} // ycsbc

#include <iostream>
#include <vector>
#include <thread>
#include <map>
#include <atomic>
#include <cassert>
#include <algorithm>

class TransactionRecord 
{
private:
    int64_t Type; // determine `insert` or `update` or `delete`
    int64_t Value;
    int64_t RelationId;

    int64_t TupleId;
    int64_t NumTuples;
    int64_t AttributeMask; // use for update

    std::vector<int64_t> TupleIds; // use for bulk insert

public:
    TransactionRecord(int64_t t) : Type(t) {};
    TransactionRecord(){};
    ~TransactionRecord(){};
    
    void SetValue(int64_t value) {Value = value;};
    int64_t GetType() {return Type;};
    int64_t GetValue() {return Value;};

    std::atomic<TransactionRecord*> NextPtr;
};

static std::map<int64_t, std::atomic<TransactionRecord*>> data;
static std::vector<time_t> active_transactions; // manage active transaction's start timestamps;


// TODO: rethink how to have these type
int64_t insert_type = 1;
int64_t update_type = 2;
int64_t delete_type = 3;

int64_t read(int64_t key)
{
    // read data from data lists
    TransactionRecord* find_version = data[key];
    if (find_version == nullptr) {
        return 0;
    }
    else {
        return find_version->GetValue();
    }
    
};

void update(int64_t key, int64_t value)
{
    std::cout << "[update] " << "key:" << key << ", value:" << value << std::endl;
    
    if (data[key] == nullptr) {
        std::cout << "key(" << key << ") is not exist." << std::endl;
        return;
    }
    
    TransactionRecord* new_head = new TransactionRecord(update_type);
    new_head->SetValue(value);

    // new transaction record is version record's head
    TransactionRecord* old_head;
    do {
        new_head->NextPtr = old_head = data[key];
    }while (!data[key].compare_exchange_weak(old_head, new_head));
    
    // TODO: GC(foreground)

    return;
};

void insert(int64_t key, int64_t value)
{
    std::cout << "[insert] " << "key:" << key << ", value:" << value << std::endl;

    // append transaction lists(thread_local)
    TransactionRecord* new_head = new TransactionRecord(insert_type);
    new_head->NextPtr = nullptr;
    new_head->SetValue(value);

    TransactionRecord* tmp = nullptr;
    if (!data[key].compare_exchange_weak(tmp, new_head)) {
        std::cout << "key(" << key << ") is already exist." << std::endl;
    }
    return;
};

// TODO
void bulk_insert() {
    return;
};

void test_insert_and_update() {
    std::cout << "[RUN TEST] insert -> update" << std::endl;
    
    int64_t key          = 1;
    int64_t first_value  = 1;
    int64_t second_value = 2;
    
    insert(key, first_value);
    update(key, second_value);
    
    TransactionRecord* cur_record = data[key];
    TransactionRecord* pre_record = cur_record->NextPtr;

    assert(cur_record->GetValue() == second_value);
    assert(pre_record->GetValue() == first_value);

    std::cout << "[FINISH TEST] insert -> update" << std::endl;
    
    return;
};

// foreground garbage collection
// refer 4.3 Eager Prunning of Obsolute Versions

void steam(int64_t key) {
    // active transaction's copy
//     std::vector<time_t> temp(active_transactions.size());
//     std::copy(active_transactions.begin(), active_transactions.end(), temp.begin());

//     std::sort(temp.begin(), temp.end());
    
//     TransactionRecord v_cur = data[key].version_record;

//     for (int i=0; i<temp.size(); i++) {
//         time_t cur_t = temp[i];
//         while (cur_t < )
//     }
//     return;
}

int main()
{
    // std::thread t1(test_insert_and_update);
    std::thread t1(insert, 1, 1);
    // std::thread t1(update, 1, 2);
    std::thread t2(insert, 2, 3);
    std::thread t3(update, 1, 4);

    t1.join();
    t2.join();
    t3.join();

    // std::cout << data[1].value << std::endl;
    // std::cout << data[1].version_record->GetNextPtr()->GetValue() << std::endl;

    return 0;
}
#include <dLevelDB.h>
#include <iostream>

int main() {
    std::cout << "Starting" << std::endl;
    dLevelDB db;

    db.put("Key1", "Val1");
    db.put("Key2", "Val2");

    std::string val = db.get("Key1");
    std::cout << val << std::endl;
    std::cout << db["Key2"] << std::endl;

    return 0;
}

#pragma once

#include <iostream>

class Algo {
    public:


    static const std::string API_KEY;
    static const std::string API_SECRET;
    static const std::string HOST;
    static const std::string MARK_PRICE_HOST;
    static const std::string TEST_HOST;
    static const std::string PORT;
    static const std::string TARGET;
    static const std::string TEST_API_KEY;
    static const std::string TEST_API_SECRET;
    static const double TP;
    static const double SL;


    Algo();
    ~Algo();

    void connect();
    void algo_TP_orders(const std::string& side, const std::string& symbol, double quantity, double entry_price, double tp);
    void algo_SL_orders(const std::string& side, const std::string& symbol, double quantity, double entry_price, double sl);

    void query_position();


private:


};
cmake_minimum_required(VERSION 3.22.1)
project(Trading_Layer)

set(CMAKE_CXX_STANDARD 20)

# Dependencies
find_package(nlohmann_json REQUIRED)
find_package(Boost 1.83 REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

add_executable(Trading_Layer
        main.cpp
        src/binance_websocket.cpp
        src/file_monitoring.cpp
        src/tools.cpp
        src/system_logger.cpp
        src/logger.cpp
        src/telegram.cpp


)

target_include_directories(Trading_Layer PRIVATE
        ${CURL_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        /opt/homebrew/include              # cppzmq + zeromq
        /opt/homebrew/Cellar/nlohmann-json/3.12.0/include
        /opt/homebrew/Cellar/boost/1.88.0/include
        /opt/homebrew/Cellar/openssl@3/3.5.0/include
)

target_link_directories(Trading_Layer PRIVATE
        /opt/homebrew/lib
        /opt/homebrew/Cellar/zeromq/4.3.5_2/lib
)

target_link_libraries(Trading_Layer PRIVATE
        ${CURL_LIBRARIES}
        nlohmann_json::nlohmann_json
        Boost::boost
        OpenSSL::SSL
        OpenSSL::Crypto
        zmq
#        uuid
)
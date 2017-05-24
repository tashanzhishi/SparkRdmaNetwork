#include <iostream>
#include <utility>
#include <map>
#include <set>
#include <string>
#include <cstdint>
#include <cstdio>

using namespace std;
/*
class A {
public:
  void print() {
    cout << "a = " << a << endl;
  };

  void testB() {
    B* b = new B(20);
    b->print();
    b->setA(30);
  }

  static A* getA() {
    if (instance == nullptr)
      instance = new A(10);
    return instance;
  }

private:
  friend class B;
  class B {
  public:
    B(int x) : b(x) {}
    void print() {
      cout << "b = " << b << endl;
    };
    void setA(int x) {
      instance->a = x;
    }
  private:
    int b;
  };

  A(int x) : a(x) {}
  A(A&) = delete;
  A&operator=(A&) = delete;

  int a;
  static A* instance;
};

A* A::instance = nullptr;


void test1() {
  A* a = A::getA();
  a->print();
  a->testB();
  a->print();
}

void test2() {
  map<int, pair<int, int>> int2pair;
  int n = 5;
  for (int i = 0; i < n; ++i) {
    int2pair[i] = pair<int, int>(i, i);
  }
  for (auto &kv : int2pair) {
    cout << kv.first << " -> [" << kv.second.first << "," << kv.second.second << "]" << endl;
  }
}

void test3() {
  set<void*, greater<void*>> sss;
  int n = 6;
  char a[10][10];

  for (int i = 0; i < n; ++i) {
    for (int j = 0; j < i+3; ++j) {
      a[i][j] = '0' + j;
    }
    a[i][i+2] = '\0';

    sss.insert(a[i]);
    cout << (void*)a[i] << " " << a[i] << endl;
  }
  for (auto &value : sss) {
    cout << value << endl;
  }
  cout << endl;

  void *b = ((void*)a[2])+10;
  cout << b << endl;

  cout << *sss.lower_bound(b) << endl;
  cout << *sss.upper_bound(b) << endl;
}

map<string, int> ip2int;
void test_string() {
  string s1 = "172.16.0.11";
  string s2 = "172.16.0.11";
  ip2int[s1] = 123;
  cout << ip2int[s2] << endl;
  string s3(nullptr);
  cout << s3 << endl;
}*/


/*#include <thread>
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
typedef boost::shared_lock<boost::shared_mutex> ReadLock;
typedef boost::unique_lock<boost::shared_mutex> WriteLock;
boost::shared_mutex rw_mutex;
map<int, int> sh;
int a;
void reader(int i) {
  usleep(100);
  ReadLock rd(rw_mutex);
  sh[i] = a;
  cout << i << ": read " << a << endl;
}
void writer(int i) {
  WriteLock wr(rw_mutex);
  a++;
  usleep(1);
  sh[i] = a;
  cout << i << ": write " << a << endl;
}
void test_lock() {
  const int read_num = 32;
  const int thread_num = 40;
  std::thread *ths[thread_num];
  for (int i = 0; i < thread_num; ++i) {
    if (i < read_num)
      ths[i] = new std::thread(reader, i);
    else
      ths[i] = new std::thread(writer, i);
  }

  for (int i = 0; i < thread_num; ++i) {
    ths[i]->join();
    delete ths[i];
  }
  for (auto &kv : sh) {
    cout << kv.first << " " << kv.second << endl;
  }
}

#include <atomic>
#include <thread>
using namespace std;
atomic_int add(0);
void test_atomic_thread_func() {
  int x = 0, y = 0;
  for (int i = 0; i < 1000; ++i) {
    x = static_cast<int>(add);
    y = atomic_fetch_add(&add, i);
    if (x != y) {
      cout << "hehe" << endl;
    }
  }
  cout << "y = " << y << endl;
}
void test_atomic() {
  thread thd[10];
  for (int i = 0; i < 10; ++i) {
    thd[i] = thread(test_atomic_thread_func);
  }
  for (int i = 0; i < 10; ++i) {
    thd[i].join();
  }
  int x = static_cast<int>(add);
  cout << add << " " << x << endl;
}*/

class tx {
public:
  tx(){}
  map<int, int> hehe;
};
void test_map() {
  tx x;
  x.hehe[1] = 1;
  for (auto &kv : x.hehe)
    cout << kv.first << " " << kv.second << endl;
}



int main() {
  //test1();
  //test2();
  //test3();
  //test_string();
  //test_lock();
  //for (auto &kv : ip2int)
  //  cout << kv.first << " " << kv.second << endl;
  //test_atomic();
  //test_map();
  return 0;
}
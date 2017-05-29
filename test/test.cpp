#include <iostream>
#include <utility>
#include <map>
#include <set>
#include <string>
#include <vector>
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
}

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

void test_vector() {
  int a[] = {1,2,3,4,5,6};
  vector<int> b(a, a+ sizeof(a)/ sizeof(int));
  for (auto &value : b)
    cout << value << " ";
  cout << endl;

  int *c = &b[0];
  int len = b.size();
  for (int i = 0; i < len; ++i)
    cout << c[i] << " ";
  cout << endl;
}*/

/*void test_while() {
  int ret = 1;
  do {
    if (ret < 10) {
      cout << ret << endl;
      ret++;
      continue;
    }
    cout << ret << endl;
  } while (0);
}
#include <queue>
#include <functional>
int usual_func(int a) {
  cout << "usual " << a << endl;
  return a=10;
}
class func_class {
public:
  func_class():a(20) {}
  int class_func(int x) {
    cout << "class " << x+a << endl;
    return x+a;
  }
private:
  int a;
};
void test_function() {
  queue<function<int(int)>> func_qee;
  func_qee.push(usual_func);

  function<int(int)> xxx(usual_func);


  func_class obj;
  func_qee.push(std::bind(&func_class::class_func, obj, std::placeholders::_1));
  while (!func_qee.empty()) {
    cout << func_qee.front()(11) << endl;
    func_qee.pop();
  }
}

#include <thread>
class test{
public:
  void print(){ cout << "hehe" << endl; }
  static thread sth;
};
thread test::sth;
void func(int a) {
  cout << "func " << a << endl;
}
void test_static_thread() {
  test t;
  test::sth = thread(func, 3);
  test::sth.join();

  t.print();
}
*/
#include <thread>
#include <functional>
#include <boost/thread/thread_pool.hpp>
#include <boost/thread.hpp>
typedef function<void(int)> vi_func;
class test {
public:
  test() : pool_(10) {}
  ~test() {pool_.close();}
  void run() {
    vi_func f = bind(&test::thread_func, this, std::placeholders::_1);
    pool_.submit(bind(f, 3));
  }
  void thread_func(int a) {
    cout << a << " thread2 " << this_thread::get_id() << endl;
  }
  boost::basic_thread_pool pool_;
};
void test_boost_thread_pool() {
  test x;
  x.run();
  //boost::basic_thread_pool pool(10);
  //function<void(int)> func = thread2;
  //for (int i = 0; i < 1; ++i) {
    //pool.submit(func);
    //pool.submit(bind(thread2, 30));
  //}
  //auto x = bind(thread2, 3);
  //x(1);
  //pool.close();
}


int main(int argc, char *argv[]) {
  //test1();
  //test2();
  //test3();
  //test_string();
  //test_lock();
  //for (auto &kv : ip2int)
  //  cout << kv.first << " " << kv.second << endl;
  //test_atomic();
  //test_map();
  //test_vector();
  //test_while();
  //test_function();
  //test_static_thread();
  test_boost_thread_pool();
  return 0;
}
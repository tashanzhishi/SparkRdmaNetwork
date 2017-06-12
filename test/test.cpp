#include <iostream>
#include <utility>
#include <map>
#include <set>
#include <string>
#include <queue>
#include <vector>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <unistd.h>

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
}

void test_while() {
  int ret = 1;
  do {
    ret = 0;
    if (ret == 0)
      continue;
    cout << ret << endl;
  } while (ret );
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
#include <functional>
class test{
public:
  test() {
    function<void()> func = bind(&test::print, this);
    sth = thread(func);
  }
  ~test() {
    sth.join();
  }
  void print(){ cout << "hehe" << endl; }
  thread sth;
};
void test_thread() {
  test t;
  sleep(1);
}

#include <thread>
#include <functional>
#include <boost/thread/thread_pool.hpp>
#include <boost/thread.hpp>
typedef function<void(int)> vi_func;
class test {
public:
  test() {}
  ~test() {}
  void run() {
    vi_func f = bind(&test::thread_func, this, std::placeholders::_1);
    for (int i = 0; i < 5; ++i) {
      pool_.submit(bind(f, i));
    }
  }
  void thread_func(int a) {
    cout << a << " thread " << this_thread::get_id() << endl;
  }
  static boost::basic_thread_pool pool_;
};
boost::basic_thread_pool test::pool_(5);
void test_boost_thread_pool() {
  test x, y;
  x.run();
  y.run();
  test::pool_.close();
}


#include <thread>
#include <memory>
thread x;
void handle_thread(shared_ptr<queue<char*>> qee) {
  cout << qee->front() << endl;
}
void test_local_queue() {
  shared_ptr<queue<char*>> local_queue(new queue<char*>);
  char *temp = new char[10];
  strcpy(temp, "hehhe");
  local_queue->push(temp);
  cout << local_queue->front() << endl;
  x = thread(handle_thread, local_queue);
  sleep(1);
  x.join();
}


#include <boost/lockfree/queue.hpp>
#include <boost/thread/thread_pool.hpp>
#include <atomic>
#include <functional>
boost::lockfree::queue<int> lf_queue(128);
atomic_int cnt(0);
void producter(int num) {
  for (int i = 0; i < 10; ++i) {
    int x = atomic_fetch_add(&cnt, 1);
    lf_queue.push(x+1);
    cout << "push " << x+1 << endl;
  }
  cout << "push end" << endl;
}
void consumer(int num) {
  for (int i = 0; i < num; ++i) {
    if (lf_queue.empty())
      break;
    int ret;
    lf_queue.pop(ret);
    cout << "         pop " << ret << endl;
  }
  cout << "pop end" << endl;
}
void test_boost_lock_free() {
  boost::basic_thread_pool pool(10);
  for (int i = 0; i < 6; ++i) {
    pool.submit(bind(producter, 10));
  }
  for (int i = 0; i < 4; ++i) {
    pool.submit(bind(consumer, 10));
  }
  sleep(1);
  pool.close();
  cout << "all end" << endl;
}

struct arr {
  int a;
  void print() {cout << a << endl;}
  arr(int x) : a(x) {}
  arr() : a(-1) {}
};
void test_class_array() {
  arr *x = new arr[3];
  x[0].print();
  for(int i=0; i<3; i++) {
    new(x+i) arr(i);
    x[i].print();
  }
}
*/
class hehe {
 public:
  hehe() {
    s = new char[10];
    strcpy(s, "hello\n");
  }
  ~hehe() {delete[] s;}
  void print() {cout << s << endl;}
 private:
  char *s;
};
class SingletonClass {
 public:
  void print() {
    cout << a << " " << b <<endl;
    c.print();
  }
  static SingletonClass* GetClassInstance() {
    if (instance == nullptr) {
      instance = new SingletonClass();
    }
    return instance;
  }
 private:
  class gc {
   public:
    ~gc() {
      if (SingletonClass::instance) {
        delete SingletonClass::instance;
      }
    }
  };
  static gc gc_;

  SingletonClass() : a(3),b(4),c() {}
  ~SingletonClass() {
    cout << "wang" << endl;
  }

  static SingletonClass *instance;
  int a, b;
  hehe c;
};
SingletonClass* SingletonClass::instance = nullptr;

void test_singleton_class() {
  SingletonClass *instance = SingletonClass::GetClassInstance();
  instance->print();
};


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
  //test_thread();
  //test_boost_thread_pool();
  //test_local_queue();
  //test_boost_lock_free();
  //test_class_array();
  test_singleton_class();

  return 0;
}
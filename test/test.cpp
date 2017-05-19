#include <iostream>
#include <utility>
#include <map>
#include <set>
#include <string>

using namespace std;

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

void test_string() {
  string s1 = "172.16.0.11";
  string s2 = "172.16.0.11";
  map<string, int> ip2int;
  ip2int[s1] = 123;
  cout << ip2int[s2] << endl;
  string s3 = nullptr;
  if (s3 == nullptr)
    cout << "hehe\n";
}

int main() {
  //test1();
  //test2();
  //test3();
  test_string();
  return 0;
}
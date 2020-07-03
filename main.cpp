#include <string>
#include "core/global_definitions.hpp"
#include "core/base_column.hpp"
#include "core/column_base_typed.hpp"
#include "core/column.hpp"
#include "core/compressed_column.hpp"

/*
 * Include the query files here
 */
#include "queries/q1.h"
#include "queries/q2.h"

using namespace CoGaDB;
using namespace std;

int* compiledQ4(boost::shared_ptr<Column<int> > column_ptr1, boost::shared_ptr<Column<int> > column_ptr2) {
	std::vector<int> array_value1(column_ptr1->size());
	for (int i = 0; i < column_ptr1->size(); i++) {

		array_value1[i] = boost::any_cast<int>(column_ptr1->get(i));
	}

	//second array
	std::vector<int> array_value2(column_ptr2->size());
	for (int i = 0; i < column_ptr2->size(); i++) {

		array_value2[i] = boost::any_cast<int>(column_ptr2->get(i));
	}

	//result array 
	std::vector<int> result;
	for (int i = 0; i < column_ptr1->size(); i++) {
		if (find(array_value2.begin(), array_value2.end(), array_value1[i]) != array_value2.end())

			result.push_back(array_value1[i]);

	}
	cout << "\nCompiled Result: \n";
	for (int i = 0; i < result.size(); i++) {
		cout << " " << result[i];
	}
	return &result[0];
}

int testSeletion()
{
	// code to test Selection operator
	boost::shared_ptr<Column<int> > column_ptr(new Column<int>("int column", INT));
	std::vector<int> reference_data(100);
	fill_column<int>(column_ptr, reference_data);

	scan  s(column_ptr);
	//selection sel(&s);
	selection sel(&s, 50, ValueComparator::LESSER);
	//selection sel(&s, 49, 51);
	reduce r(&sel);

	r.open();
	int data = r.next();
	int summa;
	do {
		summa = data;
		data = sel.next(); //summa used as the last value of data becomes EOL
	} while (data != EOL);
	r.close();

	cout << "\nVolcano Result (Selection with Reduce): \n";

	int summation = compiledQ1(column_ptr);

	std::cout << std::memcmp(&summation, &summa, 2) << std::endl;

	return 0;

}

int  testJoin() {
	// Join
  //boost::shared_ptr<Column<int> > column_ptr;//Input pointer
	boost::shared_ptr<Column<int> > column_ptr1(new Column<int>("int column", INT));
	std::vector<int> reference_data(100);
	fill_column<int>(column_ptr1, reference_data);

	boost::shared_ptr<Column<int> > column_ptr2(new Column<int>("int column", INT));
	fill_column<int>(column_ptr2, reference_data);

	scan  s1(column_ptr1);
	Join j(&s1, column_ptr2);
	j.open();
	std::vector<int> data;
	int val;
	while (true)
	{
		val = j.next();
		if (val == EOL) break;
		data.push_back(val); //summa used as the last value of data becomes EOL
	}
	j.close();

	cout << "\nVolcano Result (Join): \n";
	for (int i = 0; i < data.size(); i++) {
		cout << " " << data[i];
	}
	int* volcanoResult = &data[0];

	int* compiledResult = compiledQ4(column_ptr1, column_ptr2);
	cout << endl;
	std::cout << std::memcmp(volcanoResult, compiledResult, 2) << std::endl;//replace 1 with column size

	return 0;
}

int main(){

	testSeletion();
	testJoin();  

	return 0;
}



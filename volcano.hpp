#pragma once

#include <string>
#include "../core/global_definitions.hpp"
#include "../core/base_column.hpp"
#include "../core/column_base_typed.hpp"
#include "../core/column.hpp"
#include "../core/compressed_column.hpp"

#include "operator.hpp"

namespace CoGaDB {



    class volcanoOperator {

    public:
        volcanoOperator* child;
        volcanoOperator() {
            this->child = NULL;
        }
        virtual void open() = 0;
        virtual int next() = 0;
        virtual void close() = 0;
        ~volcanoOperator() {};

    };

    class scan : public volcanoOperator {

        size_t i;
        boost::shared_ptr<Column<int> > column_ptr;

    public:

        scan() {}

        scan(boost::shared_ptr<Column<int> > ptr) {

            this->column_ptr = ptr;
        }

        void open() {

            i = 0;
            std::cout << "open Column pointer" << std::endl;
            std::cout << "Column Size: " << column_ptr->size() << std::endl;
        };

        int next() {

            if (column_ptr->size() > i)

                return boost::any_cast<int>(column_ptr->get(i++));
            return EOL;
        };

        void close() {

            column_ptr.reset();
            i = 0;
        }
    };

    class selection : public volcanoOperator {

        int operand;

        int nMinValue;  // To store min value for range query.
        int nMaxValue;  // To store max value for range query.
        bool bRangeComparison = false;
        ValueComparator objValComparator;

    public:

        selection() {}

        // This constructor forms query that defined only floor or ceil bound. (x,y] or [x,y)
        // selection of single values
        selection(volcanoOperator* child, int oper = 50, ValueComparator objValComparator = ValueComparator::GREATER) {

            this->child = child;
            this->operand = oper;
            this->objValComparator = objValComparator;
        }
        // selection between rangevalues (min,max)
        selection(volcanoOperator* child, int nMinValue, int nMaxValue) {
            this->child = child;
            this->nMinValue = nMinValue;
            this->nMaxValue = nMaxValue;
            this->bRangeComparison = true;
        }

        void open() {

            this->child->open();
        }

        int next() {
            int data = child->next();
            while (data != EOL) {

                // if data is in the range, then return data
                if (this->bRangeComparison) {
                    if (data > this->nMinValue && data < this->nMaxValue)
                        return data;
                }

                // compare the value and return data
                switch (this->objValComparator)
                {
                case ValueComparator::EQUAL:
                    if (data == this->operand)
                        return data;
                    break;
                case ValueComparator::LESSER:
                    if (data < this->operand)
                        return data;
                    break;

                case ValueComparator::GREATER:
                    if (data > this->operand)
                        return data;
                    break;
                }

                data = child->next();
            }
            return EOL;
        }
        void close() {}
    };

    class reduce : public volcanoOperator {

        int sum;
        bool flag;

    public:

        reduce() {}
        reduce(volcanoOperator* child) {

            flag = false;
            this->child = child;
            sum = 0;
        }

        void open() {

            this->child->open();

            int data = child->next();
            while (data != EOL) {
                sum += data;
                data = child->next();
            }
        }

        int next() {

            if (!flag) {
                flag = true;
                return sum;
            }

            return EOL;

        }
        void close() {
        }
    };

    class Join : public volcanoOperator {

    public:
        boost::shared_ptr<Column<int> > ptrRefCol;
        // vtRefvalues is a copy of col2
        std::vector<int> vtRefValues;
        // vtActValues is a copy of col1
        std::vector<int> vtActValues;
        int nIndex;

        Join() { nIndex = 0; }
        Join(volcanoOperator* child) {

            this->child = child;
            nIndex = 0;
        }
        // join column1 with column2 : column1 reads from Scan, column2 has a pointer
        Join(volcanoOperator* child, boost::shared_ptr<Column<int> > ptrRefCol) {
            for (int i = 0; i < ptrRefCol->size(); i++) {
                vtRefValues.push_back(boost::any_cast<int>(ptrRefCol->get(i)));
            }
            this->ptrRefCol = ptrRefCol;
            this->child = child;
            nIndex = 0;
        }

        void open() {
            this->child->open();
            int data = this->child->next();
            while (data != EOL) {
                this->vtActValues.push_back(data);
                data = child->next();
            }
        }

        int next() {
            while (this->vtActValues.size() > nIndex) {
                // check if the value of column1 exists in column2
                if (find(this->vtRefValues.begin(), this->vtRefValues.end(), this->vtActValues[nIndex]) != this->vtRefValues.end()) {
                    return this->vtActValues.at(nIndex++);
                }
                ++nIndex;
            }
            return EOL;
        }

        void close() {
            ptrRefCol = NULL;
        }
    };

    class GroupedAggregation : public volcanoOperator {

    public:
       
        GroupedAggregation() {}
        GroupedAggregation(volcanoOperator* child) {
            this->child = child;
        }

        void open() {
           
                }
             
         
        int next() {

        }
        
        void close() {
        }
    };

    class sorting : public volcanoOperator {
       
    public:
        
        sorting() {}
        sorting(volcanoOperator* child) {

            this->child = child;
        }

        void open() {
            

            }
            

        int next() {
           
            }
           

        void close() {
        }
    };
}
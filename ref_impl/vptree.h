// A VP-Tree implementation, by Steve Hanov. (steve.hanov@gmail.com)
// Released to the Public Domain
// Based on "Data Structures and Algorithms for Nearest Neighbor Search" by Peter N. Yianilos
#include <stdlib.h>
#include <algorithm>
#include <vector>
#include <set>
#include <stdio.h>
#include <queue>
#include <limits>

template<typename T, typename S, S (*distance)( const T&, const T& )>
class VpTree
{
public:
    VpTree() : _root(0) {}

    ~VpTree() {
        fprintf(stderr, "%d / %d = %.3f, %d\n", perf_nodes_visited, perf_num_searched, (double) perf_nodes_visited / perf_num_searched, _items.size());
        delete _root;
    }

    void create( const std::vector<T>& items ) {
        delete _root;
        _items = items;
        _root = buildFromPoints(0, items.size());

        perf_nodes_visited = 0;
        perf_num_searched = 0;
    }

    // now S must be int, not event unsigned int!
    void search( const T& target, S tau,
        std::set<T>* results)
    {
        std::priority_queue<HeapItem> heap[tau];

        _tau = tau; // std::numeric_limits<S>::max();
        int visited = search(_root, target, heap);
        perf_nodes_visited += visited;
        perf_num_searched += 1;

        for (S i = 0; i < tau; i++) {
            while( !heap[i].empty() ) {
                results[i].insert(_items[heap[i].top().index] );
                heap[i].pop();
            }
        }
    }

private:
    int perf_nodes_visited;
    int perf_num_searched;
    std::vector<T> _items;
    S _tau;

    struct Node
    {
        int index;
        S threshold;
        Node* left;
        Node* right;

        Node() :
            index(0), threshold(0.), left(0), right(0) {}

        ~Node() {
            delete left;
            delete right;
        }
    }* _root;

    struct HeapItem {
        HeapItem( int index, S dist) :
            index(index), dist(dist) {}
        int index;
        S dist;
        bool operator<( const HeapItem& o ) const {
            return dist < o.dist;
        }
    };

    struct DistanceComparator
    {
        const T& item;
        DistanceComparator( const T& item ) : item(item) {}
        bool operator()(const T& a, const T& b) {
            return distance( item, a ) < distance( item, b );
        }
    };

    Node* buildFromPoints( int lower, int upper ) // [lower, upper)
    {
        if ( upper == lower ) {
            return NULL;
        }

        Node* node = new Node();
        node->index = lower;

        if ( upper - lower > 1 ) {

            // choose an arbitrary point and move it to the start
            int i = (int)((double)rand() / RAND_MAX * (upper - lower - 1) ) + lower;
            std::swap( _items[lower], _items[i] );

            int median = ( upper + lower ) / 2;

            // partitian around the median distance
            std::nth_element(
                _items.begin() + lower + 1,
                _items.begin() + median,
                _items.begin() + upper,
                DistanceComparator( _items[lower] )); // [lower, i] < [lower, median]: left; [lower, i] >= [lower, median]: right

            // what was the median?
            node->threshold = distance( _items[lower], _items[median] );

            node->index = lower;
            node->left = buildFromPoints( lower + 1, median );
            node->right = buildFromPoints( median, upper );
        }

        return node;
    }

    // return number of nodes visited
    int search( Node* node, const T& target,
                 std::priority_queue<HeapItem>* heap )
    {
        int visited = 0;
        if ( node == NULL ) return 0;

        S dist = distance( _items[node->index], target );

        // fprintf(stderr, "%d: %s %s\n", dist, _items[node->index].c_str(), target.c_str());
        //printf("dist=%g tau=%gn", dist, _tau );

        if ( dist < _tau ) {
            heap[dist].push(HeapItem(node->index, dist));
        }

        if ( node->left == NULL && node->right == NULL ) {
            return 1;
        }

        if ( dist < node->threshold ) {
            if ( dist - _tau < node->threshold ) {
                visited += search( node->left, target, heap );
            }

            if ( dist + _tau > node->threshold ) {
                visited += search( node->right, target, heap );
            }

        } else {
            if ( dist + _tau > node->threshold ) {
                visited += search( node->right, target, heap );
            }

            if ( dist - _tau < node->threshold ) {
                visited += search( node->left, target, heap );
            }
        }
        return visited;
    }
};

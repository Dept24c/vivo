# Vivo
* [About](#about)
* [Installation](#installation)
* [Vivo Concepts](#vivo-concepts)
  * [Paths](#paths)
  * [Subscription Maps](#subscription-maps)
  * [Update Commands](#update-commands)
  * [Async API](#async-api)
* [API](#api)
  * [def-component](#def-component)
  * [set-state!](#set-state)
  * [vivo-client](#vivo-client)
  * [subscribe!](#subscribe)
  * [unsubscribe!](#unsubscribe)
  * [update-state!](#update-state)
  * [use-vivo-state](#use-vivo-state)
* [Development](#development)
* [License](#license)



# About
Vivo is a framework for building connected applications.

***Vivo is in an experimental and rapidly-changing state.
Use at your own risk.***

# Installation
In deps.edn:
```clojure
{:deps {com.dept24c/vivo {:git/url "https://github.com/Dept24c/vivo.git"
                          :sha "xxx"}}}
```

# Vivo Concepts

## Local + System State
Vivo enables easy access to both local and system state. In the Vivo context,
local state is local to the client and is not shared. System state is shared
across all clients in the system. When any local or system state changes,
any subscribed clients are automatically updated with the current state. This
makes creating live, connected applications much easier.

## Paths
State paths are a sequence of keys that index into the state data structure.
These keys can be keywords, strings, or integers, depending on the specific
state data structure. Keyword keys may or may not have namespaces. A path
must start with either `:sys` (system state) or `:local` (local state).
Some examples:
* `[:local :user-id]`
* `[:local :score-info :high-score]`
* `[:sys :users "my-user-id" :user/name]`
* `[:sys :msgs 0]`

### End-relative Indexing (Sequences only)
For paths referring to sequence data types, the path can use either
front-relative indexing, .e.g.:

* `[:sys :msgs 0]` - Refers to the first msg in the list
* `[:sys :msgs 1]` - Refers to the second msg in the list

or end-relative indexing, e.g.:

* `[:sys :msgs -1]` - Refers to the last msg in the list
* `[:sys :msgs -2]` - Refers to the penultimate msg in the list

## Subscription Maps
Subscription maps are used to specify a subscription to Vivo state. Here is
an example subscription map:
```clojure
{user-id [:local :user/id]
 user-name [:sys :users user-id :user/name]
 avatar-url [:sys :users user-id :user/avatar-url]}
```
A subscription map's keys are Clojure symbols and the values are
[paths](#paths). The paths are used to index into Vivo state.
Vivo then binds the value of the state at
the specified path to the appropriate symbol.
For example, in the subscription map above, the `user-id` symbol will be
bound to the value found in the Vivo state at `[:local :user/id]`.

Note that symbols may be used in a path. If a symbol is used in a path,
it must be defined by another map entry. For example, in the subscription
map above, the `user-id` symbol is used in both the `user-name`
and `avatar-url` paths.

Order is not important in the map; symbols can be defined in any order.

## Update Commands
An update command is a map with three keys:
* `:path`: The [path](#paths) on which the update command will operate;
e.g. `[:local :page]`
* `:op`: One of the supported update operations: (`:set`, `:remove`,
`:insert-before`, `:insert-after`, `:plus`, `:minus`, `:multiply`, `:divide`,
`:mod`)
* `:arg`: The command's argument

For example:

## Async API
In order to work well in browsers, the Vivo API is asynchronous. Most Vivo
functions have three forms:
* A simple form: `(update-state! vc update-commands)` - Return value is ignored.
* A callback form: `(update-state! vc update-commands cb)` - Return value is
provided by calling the given callback `cb`.
* A channel form: `(<update-state! vc update-commands)` - Returns a
core.async channel, which will yield the function's return value.


# API
---

## `vivo-client`
```clojure
(vivo-client)
(vivo-client opts)
```
Creates a vivo client with the given options, if any. An application
should have exactly one Vivo client, which must be passed to all Vivo
functions and components.

### Parameters
* `opts`: A map of options. Supported options:
  * `:get-server-url`: A zero-arity function which returns a URL for
  the Vivo server. Can return the URL or a channel which yields the
  URL. Required if using `:sys` state.
  * `:initial-local-state`: Initial value of the `:local` state.
  Defaults to `nil`.
  * `:log-error`: A function of one argument (a string) to log errors.
  Defaults to `println`.
  * `:log-info`: A function of one argument (a string) to log information.
  Defaults to `println`.
  * `:state-cache-size`: Size of the state cache, measured in number of items.
     Defaults to 100.
  * `:sys-state-schema`: The [Lancaster](https://github.com/deercreeklabs/lancaster) schema for the `:sys` state. Required if using `:sys` state.
  * `:sys-state-source`: A map describing the source branch for the `:sys` state. Defaults to `{:temp-branch/db-id nil}`. Must be one of:
    * `{:branch branch-name}` Sets the source to the given `branch-name`.
    * `{:temp-branch/db-id nil}` Creates an empty temporary branch and sets it as the source. The branch is deleted when the Vivo client disconnects from the server. Useful for testing and staging environments.
    * `{:temp-branch/db-id db-id}` Creates a temporary branch from the given `db-id` and sets it as the source. The branch is deleted when the Vivo client disconnects from the server. Useful for testing and staging environments.

### Return Value
The created Vivo client.

### Example
```clojure
(defonce vc (vivo/vivo-client))
```

---

## `update-state!`
```clojure
(update-state! vc update-commands)
(update-state! vc update-commands cb)
(<update-state! vc update-commands)
```
Updates the state by executing the given sequence of update commands. The commands are executed in order. Atomicity is guraranteed. Either all the commands will succeed or none will.


### Parameters
* `vc`: The Vivo client instance
* `update-commands`: A sequence of [update commmands](#update-commands).

### Return Value
`true` or `false`, indicating success or failure.

### Example
This sets the local page state to `:home`:
```clojure
(vivo/update-state! vc [{:path [:local :page]
                         :op :set
                         :arg :home}])
```

### See Also
* [set-state!](#set-state)
Since `:set` is the most common update command operation, Vivo provides a convenience fn for it:
```clojure
(vivo/set-state! [:local :page] :home)
```
is shorthand for:
```clojure
(vivo/update-state! vc [{:path [:local :page]
                         :op :set
                         :arg :home}])
```

---

## `set-state!`
```clojure
(set-state! vc path arg)
(set-state! vc path arg cb)
(<set-state! vc path arg)
```
Sets the state at the given path to the given arg.

### Parameters
* `vc`: The Vivo client instance
* `path`: The state [path](#paths)
* `arg`: The value to set

### Return Value
`true` or `false`, indicating success or failure.

### Example
```clojure
(vivo/set-state! [:local :page] :home)
```
This is shorthand for:
```clojure
(vivo/update-state! vc [{:path [:local :page]
                         :op :set
                         :arg :home}])
```

### See Also
* [update-state!](#update-state)

---

## `def-component`
```clojure
(def-component component-name & args)
```
Defines a Vivo React component.

### Parameters
* `constructor-args`: A vector of constructor arguments. The first argument
* `sub-map`: Optional. A [subscription map](#subscription-maps).
must be a parameter named `vc` (the Vivo client).
* `body`: The body of the component. When evaluated, the body should
return a Hiccup data structure.

### Return Value
The defined Vivo component

### Example
```clojure
(def-component main
  [vc]
  {page [:local :page]}
  (case page
    :home (home/home vc)
    :details (details/details vc)
    :not-found (not-found vc)))
```

---

## `subscribe!`
```clojure
(subscribe! vc sub-map cur-state update-fn)
```
Creates a Vivo subscription. Note that it is rare to call this function
directly. Prefer the higher-level [def-component](#def-component) or
[use-vivo-state](#use-vivo-state), which handle subscribing and unsubscribing
directly.

### Parameters
* `vc`: The Vivo client instance
* `sub-map`: A [subscription map](#subscription-maps)
* `update-fn`: A function that will be called when the subscribed state changes. The function will recieve a single map argument. The map's keys will be the symbols from the subscription map, and the map's values will be the pieces of state indicated by the paths in the subscription map.

### Return Value
A subscription id (an integer) that can be used in [unsubscribe!](#unsubscribe) calls.

### Example
TODO

---

## `unsubscribe!`
```clojure
(unsubscribe! vc sub-id)
```
Deletes a Vivo subscription.  Note that it is rare to call this function
directly. Prefer the higher-level [def-component](#def-component) or
[use-vivo-state](#use-vivo-state), which handle subscribing and unsubscribing
directly.

### Parameters
* `vc`: The Vivo client instance
* `sub-id`: The subscription id (an integer) returned from the [subscribe!](#subscribe) call.

### Example
```clojure
(vivo/unsubscribe 789)
```

---

## `use-vivo-state`
```clojure
(use-vivo-state vc sub-map)
```
React custom hook for using Vivo state. Automatically re-renders the component
when any of the subscribed state changes. Returns a state map, which is a
map with the same keys as the [subscription map](#subscription-maps),
but with state values.

The React [Rules of Hooks](https://reactjs.org/docs/hooks-rules.html) apply.

### Parameters
* `vc`: The Vivo client instance
* `sub-map`: A [subscription map](#subscription-maps)

### Example
```clojure
(vivo/unsubscribe 789)
```

---

## `shutdown!`
```clojure
(shutdown! vc)
```
Shut down the Vivo client, closing its connection to the server. Useful
in testing and code reloading to avoid having multiple connections to the
server.

### Parameters
* `vc`: The Vivo client instance
* `sub-map`: A [subscription map](#subscription-maps)

### Example
```clojure
(vivo/unsubscribe 789)
```

# Development

* Unit tests
  * To run the clj unit tests:
    `bin/kaocha unit`
  * To run the cljs/node unit tests:
  `bin/kaocha unit-node`
  * To run the cljs/browser unit tests:
  `bin/kaocha unit-browser`

* Integration tests
  * First, start the test server: `bin/run-test-server`
  * To run the clj integration tests:
    `bin/kaocha integration`
  * To run the cljs/node integration tests:
  `bin/kaocha integration-node`
  * To run the cljs/browser integration tests:
  `bin/kaocha integration-browser`




# License
Copyright Department 24C, LLC

*Apache and the Apache logos are trademarks of The Apache Software Foundation.*

Distributed under the Apache Software License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.txt

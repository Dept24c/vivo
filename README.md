# Vivo
* [Installation](#installation)
* [About](#about)
* [Subscription Maps](#subscription-maps)
* [Update Expressions](#update-expressions)
* [API](#api)
* [License](#license)

# Installation

# About
Vivo is a framework for building connected applications.

# Subscription Maps
Subscription maps are used to specify a subscription to Vivo state.
Here is an example subscription map:
```clojure
```

# Update Expressions
Update expressions are used by the [update-state!](#update-state!) function
to update the Vivo state.

# API
---
## `state-manager`
```clojure
(state-manager root-key->state-provider)
```
Creates a state manager with the given mapping of root keys to
state providers. Each Vivo client should have exactly one
state manager, which is passed to all Vivo components.
### Parameters
* `root-key->state-provider`: A map of root keys to state providers.
There must be at least one entry in the map.

### Return Value
The created state manager.

### Example
```clojure
(defonce sm (vivo/state-manager {:local (vivo/mem-state-provider)}))
```

---
## `mem-state-provider`
```clojure
(mem-state-provider)
(mem-state-provider initial-state)
```
Creates an in-memory, non-durable state provider. Optionally takes
an initial state as an argument.
### Parameters
* `initial-state`: Optional. The initial state to store.

### Return Value
The created state provider

### Example
```clojure
(defonce sm (vivo/state-manager {:local (vivo/mem-state-provider)}))
```

---
## `update-state!`
```clojure
(update-state sm update-map)
```
Updates the state using the given update map, which is a map of paths
to [update expressions](#update-expressions) (upexes).
If order is important, the update map can be
replaced with a sequence of [path update-expression] pairs.
### Parameters
* `sm`: The Vivo state manager instance
* `update-map`: A map of paths to [update expressions](#update-expressions)

### Return Value
`nil`

### Example
```clojure
(vivo/update-state!
  sm {[:local] [:assoc :page :home]})
```

---
## `def-component`
```clojure
(def-component component-name & args)
```
Defines a Vivo React component.

### Parameters
* `sub-map`: Optional. A [subscription map](#subscription-maps)
* `constructor-args`: A vector of constructor arguments. The first argument
must be a parameter named `sm` (the state manager).
* `body`: The body of the component. When evaluated, the body should
return a Hiccup data structure.

### Return Value
The defined Vivo component

### Example
```clojure
(def-component main
  {page [:local :page]}
  [sm]
  (case page
    :home (home/home sm)
    :details (details/details sm)
    :not-found (not-found sm)))
```

---
## `def-subscriber`
```clojure
(def-subscriber subscriber-name & args)
```
Defines a non-visual Vivo subscriber.

### Parameters
* `sub-map`: A [subscription map](#subscription-maps)
* `constructor-args`: A vector of constructor arguments. The first argument
must be a parameter named `sm` (the state manager).
* `body`: The body of the subscriber. The subscriber may do anything,
including producing side effects. The return value is ignored.

### Return Value
The defined subscriber

### Example
```clojure
(vivo/def-subscriber a-subscriber
  {name [:local :name]}
  [sm ch]
  (ca/put! ch name))
```


# License
Copyright Department 24c, LLC

*Apache and the Apache logos are trademarks of The Apache Software Foundation.*

Distributed under the Apache Software License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.txt
